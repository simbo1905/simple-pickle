package io.github.simbo1905.no.framework;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Constructor;
import java.lang.reflect.RecordComponent;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static io.github.simbo1905.no.framework.Constants.*;
import static io.github.simbo1905.no.framework.Pickler.LOGGER;
import static java.nio.charset.StandardCharsets.UTF_8;

public class CompanionNew {

  public static final ConcurrentHashMap<Class<?>, Pickler<?>> REGISTRY = new ConcurrentHashMap<>();

  // In Pickler interface
  @SuppressWarnings("unchecked")
  static <T> Pickler<T> getOrCreate(Class<T> type, Supplier<Pickler<T>> supplier) {
    return (Pickler<T>) REGISTRY.computeIfAbsent(type, k -> supplier.get());
  }

  static <R extends Record> Pickler<R> manufactureRecordPickler(Class<R> recordClass) {
    final Map<Integer, MethodHandle> fallbackConstructorHandles = new HashMap<>();
    final MethodHandle[] componentAccessors;
    final RecordComponent[] components = recordClass.getRecordComponents();
    final int componentCount = components.length;
    final MethodHandles.Lookup lookup = MethodHandles.lookup();
    final MethodHandle canonicalConstructorHandle;
    try {
      // Get parameter types for the canonical constructor
      Class<?>[] parameterTypes = Arrays.stream(components)
          .map(RecordComponent::getType)
          .toArray(Class<?>[]::new);

      // Get the canonical constructor
      Constructor<?> constructorHandle = recordClass.getDeclaredConstructor(parameterTypes);
      canonicalConstructorHandle = lookup.unreflectConstructor(constructorHandle);

      componentAccessors = new MethodHandle[components.length];
      Arrays.setAll(componentAccessors, i -> {
        try {
          return lookup.unreflect(components[i].getAccessor());
        } catch (IllegalAccessException e) {
          final var msg = "Failed to access component accessor for " + components[i].getName() +
              " in record class " + recordClass.getName() + ": " + e.getClass().getSimpleName();
          LOGGER.severe(() -> msg);
          throw new IllegalArgumentException(msg, e);
        }
      });
    } catch (Exception e) {
      Throwable inner = e;
      while (inner.getCause() != null) {
        inner = inner.getCause();
      }
      final var msg = "Failed to access record components for class '" +
          recordClass.getName() + "' due to " + inner.getClass().getSimpleName() + " " + inner.getMessage();
      LOGGER.severe(() -> msg);
      throw new IllegalArgumentException(msg, inner);
    }

    // Get the canonical constructor and any fallback constructors for schema evolution
    try {
      // Get all public constructors
      final Constructor<?>[] allConstructors = recordClass.getConstructors();

      for (Constructor<?> constructor : allConstructors) {
        int currentParamCount = constructor.getParameterCount();
        MethodHandle handle;

        try {
          handle = lookup.unreflectConstructor(constructor);
        } catch (IllegalAccessException e) {
          LOGGER.warning("Cannot access constructor with " + currentParamCount +
              " parameters for " + recordClass.getName() + ": " + e.getMessage());
          continue;
        }

        // This is a potential fallback constructor for schema evolution
        if (fallbackConstructorHandles.containsKey(currentParamCount)) {
          LOGGER.warning("Multiple fallback constructors with " + currentParamCount +
              " parameters found for " + recordClass.getName() +
              ". Using the first one encountered.");
          // We keep the first one we found
        } else {
          fallbackConstructorHandles.put(currentParamCount, handle);
          LOGGER.fine("Found fallback constructor with " + currentParamCount +
              " parameters for " + recordClass.getName());
        }

      }
    } catch (Exception e) {
      final var msg = "Failed to access constructors for record '" +
          recordClass.getName() + "' due to " + e.getClass().getSimpleName() + " " + e.getMessage();
      LOGGER.severe(() -> msg);
      throw new IllegalArgumentException(msg, e);
    }

    // Capture these values for use in the anonymous class
    final MethodHandle finalCanonicalConstructorHandle = canonicalConstructorHandle;

    final Pickler.Compatibility compatibility = Pickler.Compatibility.valueOf(
        System.getProperty(Pickler.Compatibility.COMPATIBILITY_SYSTEM_PROPERTY, "NONE"));

    final String recordClassName = recordClass.getName();
    if (compatibility != Pickler.Compatibility.NONE) {
      // We are secure by default this is opt-in and should not be left on forever so best to nag
      LOGGER.warning(() -> "Pickler for " + recordClassName + " has Compatibility set to " + compatibility.name());
    }

    // we are security by default so if we are set to strict mode do not allow fallback constructors
    final Map<Integer, MethodHandle> finalFallbackConstructorHandles =
        (Pickler.Compatibility.BACKWARDS == compatibility || Pickler.Compatibility.ALL == compatibility) ?
            Collections.unmodifiableMap(fallbackConstructorHandles) : Collections.emptyMap();

    return new RecordPickler<>() {

      @Override
      public Compatibility compatibility() {
        return compatibility;
      }

      @Override
      void serializeWithMap(Map<Class<?>, Integer> classToOffset, ByteBuffer buffer, R object) {
        final var components = components(object);
        // Write the number of components as an unsigned byte (max 255)
        LOGGER.finer(() -> "serializeWithMap Writing component length length=" + components.length + " position=" + buffer.position());
        buffer.putInt(components.length);
        Arrays.stream(components).forEach(c -> WriteOperations.write(classToOffset, buffer, c));
      }

      @Override
      R deserializeWithMap(ByteBuffer buffer, Map<Integer, Class<?>> bufferOffset2Class) {
        // Read the number of components as an unsigned byte
        LOGGER.finer(() -> "deserializeWithMap reading component length position=" + buffer.position());
        final int length = buffer.getInt();
        Compatibility.validate(compatibility, recordClassName, componentCount, length);
        // This may unload from the stream things that we will ignore
        final Object[] components = new Object[length];
        Arrays.setAll(components, ignored -> WriteOperations.deserializeValue(bufferOffset2Class, buffer));
        if (componentCount < length && (Compatibility.FORWARDS == compatibility || Compatibility.ALL == compatibility)) {
          return this.staticCreateFromComponents(Arrays.copyOfRange(components, 0, componentCount));
        }
        return this.staticCreateFromComponents(components);
      }

      private Object[] components(R record) {
        Object[] result = new Object[componentAccessors.length];
        Arrays.setAll(result, i -> {
          try {
            return componentAccessors[i].invokeWithArguments(record);
          } catch (Throwable e) {
            final var msg = "Failed to access component: " + i +
                " in record class '" + recordClassName + "' : " + e.getMessage();
            LOGGER.severe(() -> msg);
            throw new IllegalArgumentException(msg, e);
          }
        });
        return result;
      }

      @SuppressWarnings("unchecked")
      private R staticCreateFromComponents(Object[] components) {
        try {
          // Get the number of components from the serialized data
          int numComponents = components.length;
          MethodHandle constructorToUse;

          if (numComponents == componentCount) {
            // Number of components matches the canonical constructor - use it directly
            constructorToUse = finalCanonicalConstructorHandle;
          } else {
            // Number of components differs, look for a fallback constructor
            constructorToUse = finalFallbackConstructorHandles.get(numComponents);
            if (constructorToUse == null) {
              final var msg = "Schema evolution error: Cannot deserialize data for " +
                  recordClassName + ". Found " + numComponents +
                  " components, but no matching constructor (canonical or fallback) exists.";
              LOGGER.severe(() -> msg);
              // No fallback constructor matches the number of components found
              throw new IllegalArgumentException(msg);
            }
          }

          // Invoke the selected constructor
          return (R) constructorToUse.invokeWithArguments(components);
        } catch (Throwable e) {
          final var msg = "Failed to create instance of " + recordClassName +
              " with " + components.length + " components: " + e.getMessage();
          LOGGER.severe(() -> msg);
          throw new IllegalArgumentException(msg, e);
        }
      }

      @Override
      public void serialize(R object, ByteBuffer buffer) {
        buffer.order(java.nio.ByteOrder.BIG_ENDIAN);
        serializeWithMap(new HashMap<>(), buffer, object);
      }

      @Override
      public R deserialize(ByteBuffer buffer) {
        buffer.order(java.nio.ByteOrder.BIG_ENDIAN);
        return deserializeWithMap(buffer, new HashMap<>());
      }

      /// This recursively descends through the object graph to find the size of the object
      /// It has its own logic to count jup the sizes that needs separate handling
      @Override
      public int sizeOf(R object) {
        throw new AssertionError("Not implemented");
      }
    };
  }

  static <S> Pickler<S> manufactureSealedPickler(Class<S> sealedClass) {
    // Get all permitted record subclasses
    final Class<?>[] subclasses = sealedInterfacePermittedRecords(sealedClass).toArray(Class<?>[]::new);

    // note that we cannot add these pickers to the cache map as we are inside a computeIfAbsent yet
    // practically speaking mix picklers into the same logical stream  is hard so preemptive caching wasteful
    @SuppressWarnings("unchecked") Map<Class<? extends S>, Pickler<? extends S>> subPicklers = Arrays.stream(subclasses)
        .filter(cls -> cls.isRecord() || cls.isSealed())
        .map(cls -> (Class<? extends S>) cls) // Safe due to sealed hierarchy
        .collect(Collectors.toMap(
            cls -> cls,
            cls -> {
              if (cls.isRecord()) {
                // Double cast required to satisfy compiler
                @SuppressWarnings("unchecked")
                Class<? extends Record> recordCls = (Class<? extends Record>) cls;
                return (Pickler<S>) manufactureRecordPickler(recordCls);
              } else {
                return manufactureSealedPickler(cls);
              }
            }
        ));

    // We do not want to use the full class name as it is very long. So we will chop of the common prefix. Note that
    // normally a sealed interface and its permitted classes are in the same package yet there are special rules for
    // Java module feature. If you move stuff around in that model you may break backwards compatibility.
    final Map<Class<? extends S>, String> shortNames = subPicklers.keySet().stream().
        collect(Collectors.toMap(
            cls -> cls,
            cls -> cls.getName().substring(
                subPicklers.keySet().stream()
                    .map(Class::getName)
                    .reduce((a, b) ->
                        !a.isEmpty() && !b.isEmpty() ?
                            a.substring(0,
                                IntStream.range(0, Math.min(a.length(), b.length()))
                                    .filter(i -> a.charAt(i) != b.charAt(i))
                                    .findFirst()
                                    .orElse(Math.min(a.length(), b.length()))) : "")
                    .orElse("").length())));

    @SuppressWarnings({"unchecked", "Convert2MethodRef"}) final Map<String, Class<? extends S>> permittedRecordClasses = Arrays.stream(subclasses)
        .collect(Collectors.toMap(
            c -> shortNames.get(c),
            c -> (Class<? extends S>) c
        ));

    return new SealedPickler<>() {

      /// There is nothing effective we can do here.
      @Override
      public Compatibility compatibility() {
        return Compatibility.NONE;
      }

      @Override
      public void serialize(S object, ByteBuffer work) {
        work.order(java.nio.ByteOrder.BIG_ENDIAN);

        if (object == null) {
          work.put(NULL.marker());
          return;
        }

        // Cast the sealed interface to the concrete type.
        @SuppressWarnings("unchecked") Class<? extends S> concreteType = (Class<? extends S>) object.getClass();

        Map<Class<?>, Integer> classToOffset = new HashMap<>();

        // write the type identifier
        writeDeduplicatedClassName(work, concreteType, classToOffset, shortNames.get(concreteType));

        // Delegate to subtype pickler
        Pickler<? extends S> pickler = subPicklers.get(concreteType);
        //noinspection unchecked
        ((RecordPickler<Record>) pickler).serializeWithMap(classToOffset, work, (Record) object);
      }

      @Override
      public S deserialize(ByteBuffer buffer) {
        buffer.order(java.nio.ByteOrder.BIG_ENDIAN);
        // if the type is NULL, return null, else read the type identifier
        buffer.mark();
        if (buffer.get() == NULL.marker()) {
          return null;
        }
        buffer.reset();

        // Read type identifier
        Class<? extends S> concreteType = resolveCachedClassByPickedName(buffer);
        // Get subtype pickler
        RecordPickler<?> pickler = (RecordPickler<?>) subPicklers.get(concreteType);
        //noinspection unchecked
        return (S) pickler.deserializeWithMap(buffer, new HashMap<>());
      }

      @Override
      public int sizeOf(S object) {
        throw new AssertionError("not implemented");
      }

      @Override
      Class<? extends S> resolveCachedClassByPickedName(ByteBuffer buffer) {
        final int classNameLength = buffer.getInt();
        final byte[] classNameBytes = new byte[classNameLength];
        buffer.get(classNameBytes);
        final String classNameShortened = new String(classNameBytes, UTF_8);
        if (!permittedRecordClasses.containsKey(classNameShortened)) {
          throw new IllegalArgumentException("Unknown subtype: " + classNameShortened);
        }
        return permittedRecordClasses.get(classNameShortened);
      }
    };
  }

  private static <S> void writeDeduplicatedClassName(ByteBuffer work, Class<? extends S> concreteType, Map<Class<?>, Integer> classToOffset, String s) {
    throw new AssertionError("Not implemented");
  }

  static byte typeMarker(Object c) {
    if (c == null) {
      return NULL.marker();
    }
    if (c.getClass().isArray()) {
      return ARRAY.marker();
    }
    if (c instanceof Enum<?>) {
      return ENUM.marker();
    }
    return switch (c) {
      case Integer ignored -> INTEGER.marker();
      case Long ignored -> LONG.marker();
      case Short ignored -> SHORT.marker();
      case Byte ignored -> BYTE.marker();
      case Double ignored -> DOUBLE.marker();
      case Float ignored -> FLOAT.marker();
      case Character ignored -> CHARACTER.marker();
      case Boolean ignored -> BOOLEAN.marker();
      case String ignored -> STRING.marker();
      case Optional<?> ignored -> OPTIONAL.marker();
      case Record ignored -> RECORD.marker();
      case Map<?, ?> ignored -> MAP.marker();
      case List<?> ignored -> LIST.marker();
      default -> throw new IllegalArgumentException("Unsupported type: " + c.getClass());
    };
  }

  /// Helper method to recursively find all permitted record classes
  static Stream<Class<?>> sealedInterfacePermittedRecords(Class<?> sealedClass) {
    if (!sealedClass.isSealed()) {
      final var msg = "Class is not sealed: " + sealedClass.getName();
      LOGGER.severe(() -> msg);
      throw new IllegalArgumentException(msg);
    }

    return Arrays.stream(sealedClass.getPermittedSubclasses())
        .flatMap(subclass -> {
          if (subclass.isRecord()) {
            return Stream.of(subclass);
          } else if (subclass.isSealed()) {
            return sealedInterfacePermittedRecords(subclass);
          } else {
            final var msg = "Permitted subclass must be either a record or sealed interface: " +
                subclass.getName();
            LOGGER.severe(() -> msg);
            throw new IllegalArgumentException(msg);
          }
        });
  }

}
