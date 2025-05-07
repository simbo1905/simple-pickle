package io.github.simbo1905.no.framework;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.reflect.Array;
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

class Companion {

  public static final Map<Class<?>, Pickler<?>> REGISTRY = new ConcurrentHashMap<>();

  // In Pickler interface
  @SuppressWarnings("unchecked")
  static <T> Pickler<T> getOrCreate(Class<T> type, Supplier<Pickler<T>> supplier) {
    return (Pickler<T>) REGISTRY.computeIfAbsent(type, k -> supplier.get());
  }

  static void write(Map<Class<?>, Integer> classToOffset, Work buffer, Object c) {
    if (c == null) {
      buffer.put(NULL.marker());
      return;
    }

    if (c.getClass().isArray()) {
      buffer.put(ARRAY.marker());

      // TODO this will write out "java.lang.String" as the type of the array. surely we can do collision detection and use String
      writeDeduplicatedClassName(buffer, c.getClass().getComponentType(), classToOffset,
          c.getClass().getComponentType().getName());

      // Write the array length
      int length = Array.getLength(c);
      buffer.putInt(length);

      if (byte.class.equals(c.getClass().getComponentType())) {
        buffer.put((byte[]) c);
      } else {
        IntStream.range(0, length).forEach(i -> write(classToOffset, buffer, Array.get(c, i)));
      }

      return;
    }

    switch (c) {
      case Integer i -> buffer.put(typeMarker(c)).putInt(i);
      case Long l -> buffer.put(typeMarker(c)).putLong(l);
      case Short s -> buffer.put(typeMarker(c)).putShort(s);
      case Byte b -> buffer.put(typeMarker(c)).put(b);
      case Double d -> buffer.put(typeMarker(c)).putDouble(d);
      case Float f -> buffer.put(typeMarker(c)).putFloat(f);
      case Character ch -> buffer.put(typeMarker(c)).putChar(ch);
      case Boolean bool -> buffer.put(typeMarker(c)).put((byte) (bool ? 1 : 0));
      case String str -> {
        buffer.put(typeMarker(c));
        final var bytes = str.getBytes(UTF_8);
        buffer.putInt(bytes.length); // FIXME MUST USE Using full int instead of byte
        buffer.put(bytes);
      }
      case Optional<?> opt -> {
        buffer.put(typeMarker(c));
        if (opt.isEmpty()) {
          buffer.put((byte) 0); // 0 = empty
        } else {
          buffer.put((byte) 1); // 1 = present
          Object value = opt.get();
          write(classToOffset, buffer, value);
        }
      }
      case Record record -> {
        buffer.put(typeMarker(c));

        // Write the class name with deduplication
        writeDeduplicatedClassName(buffer, record.getClass(), classToOffset, c.getClass().getName());

        // Get the appropriate pickler for this record type
        @SuppressWarnings("unchecked")
        RecordPickler<Record> nestedPickler = (RecordPickler<Record>) Pickler.forRecord(record.getClass());

        nestedPickler.serializeWithMap(classToOffset, buffer, record);
      }
      case Map<?, ?> map -> {
        buffer.put(typeMarker(c));

        // Write the number of entries
        buffer.putInt(map.size());

        // Write each key-value pair
        map.forEach((key, value) -> {
          // Write the key
          write(classToOffset, buffer, key);
          // Write the value
          write(classToOffset, buffer, value);
        });
      }
      case List<?> list -> {
        buffer.put(typeMarker(c));

        // Write the number of elements
        buffer.putInt(list.size());

        // Write each element
        list.forEach(element -> write(classToOffset, buffer, element));
      }
      case Enum<?> enumValue -> {
        buffer.put(typeMarker(c));
        // Write the enum class name with deduplication
        writeDeduplicatedClassName(buffer, enumValue.getClass(), classToOffset, c.getClass().getName());

        // Write the enum constant name
        String enumConstantName = enumValue.name();
        byte[] enumNameBytes = enumConstantName.getBytes(UTF_8);

        buffer.putInt(enumNameBytes.length);
        buffer.put(enumNameBytes);
      }
      default -> throw new IllegalArgumentException("Unsupported type: " + c.getClass());
    }
  }

  /// Helper method to write a class name to a buffer with deduplication.
  /// If the class has been seen before, writes a negative reference instead of the full name.
  ///
  /// @param buffer The buffer to write to
  /// @param clazz The class to write
  /// @param classToOffset Map tracking class to buffer position offset
  /// @param classNameShorted The short name of the class after taking all the record types and chopping off the common initial substring.
  static void writeDeduplicatedClassName(Work buffer, Class<?> clazz,
                                         Map<Class<?>, Integer> classToOffset, String classNameShorted) {
    // Check if we've seen this class before
    Integer offset = classToOffset.get(clazz);
    if (offset != null) {
      // We've seen this class before, write a negative reference
      int reference = ~offset;
      buffer.putInt(reference); // Using bitwise complement for negative reference
    } else {
      // First time seeing this class, write the full name
      byte[] classNameBytes = classNameShorted.getBytes(UTF_8);
      int classNameLength = classNameBytes.length;

      // Store current position before writing
      int currentPosition = buffer.position();

      // Write positive length and class name
      buffer.putInt(classNameLength);
      buffer.put(classNameBytes);

      // Store the position where we wrote this class
      classToOffset.put(clazz, currentPosition);
    }
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

  static Object deserializeValue(Map<Integer, Class<?>> bufferOffset2Class, Work buffer) {
    final byte type = buffer.get();
    final Constants typeEnum = fromMarker(type);
    return switch (typeEnum) {
      case INTEGER -> buffer.getInt();
      case LONG -> buffer.getLong();
      case SHORT -> buffer.getShort();
      case BYTE -> buffer.get();
      case DOUBLE -> buffer.getDouble();
      case FLOAT -> buffer.getFloat();
      case CHARACTER -> buffer.getChar();
      case BOOLEAN -> buffer.get() == 1;
      case STRING -> {
        final var strLength = buffer.getInt();
        final byte[] bytes = new byte[strLength];
        buffer.get(bytes);
        yield new String(bytes, UTF_8);
      }
      case OPTIONAL -> {
        byte isPresent = buffer.get();
        if (isPresent == 0) {
          yield Optional.empty();
        } else {
          Object value = deserializeValue(bufferOffset2Class, buffer);
          yield Optional.ofNullable(value);
        }
      }
      case RECORD -> { // Handle nested record
        try {
          // Read the class with deduplication support
          // FIXME: This should not do classForName as we should do that at instantiation of the pickler
          Class<?> recordClass = resolveClass(buffer, bufferOffset2Class);

          // Get or create the pickler for this class
          @SuppressWarnings("unchecked")
          RecordPickler<Record> nestedPickler = (RecordPickler<Record>) Pickler.forRecord((Class<? extends Record>) recordClass);

          // Deserialize the nested record
          yield nestedPickler.deserializeWithMap(buffer, bufferOffset2Class);
        } catch (ClassNotFoundException e) {
          final var msg = "Failed to load class: " + e.getMessage();
          LOGGER.severe(() -> msg);
          throw new IllegalArgumentException(msg, e);
        }
      }
      case NULL -> null; // Handle null values
      case ARRAY -> { // Handle arrays
        try {
          // Get the component class
          Class<?> componentType = resolveClass(buffer, bufferOffset2Class);

          // Read array length
          int length = buffer.getInt();

          // Create array of the right type and size
          final Object array = Array.newInstance(componentType, length);

          if (componentType.equals(byte.class)) {
            buffer.get((byte[]) array);
          } else {
            // Deserialize each element using IntStream instead of for loop
            IntStream.range(0, length)
                .forEach(i -> Array.set(array, i, deserializeValue(bufferOffset2Class, buffer)));
          }

          yield array;
        } catch (ClassNotFoundException e) {
          final var msg = "Failed to load component class: " + e.getMessage();
          LOGGER.severe(() -> msg);
          throw new IllegalArgumentException(msg, e);
        }
      }
      case MAP -> // Handle maps
          IntStream.range(0, buffer.getInt())
              .mapToObj(i ->
                  Map.entry(
                      Objects.requireNonNull(deserializeValue(bufferOffset2Class, buffer)),
                      Objects.requireNonNull(deserializeValue(bufferOffset2Class, buffer))))
              .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));
      case LIST -> // Handle Lists
          IntStream.range(0, buffer.getInt())
              .mapToObj(i -> deserializeValue(bufferOffset2Class, buffer))
              .toList();
      case ENUM -> { // Handle enums
        try {
          // Read the enum class with deduplication support
          Class<?> enumClass = resolveClass(buffer, bufferOffset2Class);

          // Verify it's an enum class
          if (!enumClass.isEnum()) {
            final var msg = "Expected enum class but got: " + enumClass.getName();
            LOGGER.severe(() -> msg);
            throw new IllegalArgumentException(msg);
          }

          // Read the enum constant name
          int enumNameLength = buffer.getInt();
          byte[] enumNameBytes = new byte[enumNameLength];
          buffer.get(enumNameBytes);
          String enumName = new String(enumNameBytes, UTF_8);

          // Get the enum constant using helper method with proper type witness
          yield enumValueOf(enumClass, enumName);
        } catch (ClassNotFoundException e) {
          final var msg = "Failed to load enum class: " + e.getMessage();
          LOGGER.severe(() -> msg);
          throw new IllegalArgumentException(msg, e);
        }
      }
    };
  }

  /// Helper method to read a class name from a buffer with deduplication support.
  ///
  /// @param buffer The buffer to read from
  /// @param bufferOffset2Class Map tracking buffer position to class
  /// @return The loaded class
  @Deprecated
  static Class<?> resolveClass(Work buffer,
                                      Map<Integer, Class<?>> bufferOffset2Class)
      throws ClassNotFoundException {
    LOGGER.finer(() -> "readInt componentTypeLength");
    // Read the class name length or reference
    int componentTypeLength = buffer.getInt();

    if (componentTypeLength > Short.MAX_VALUE) {
      final var msg = "The max length of a string in java is 65535 bytes, " +
          "but the length of the class name is " + componentTypeLength;
      LOGGER.severe(() -> msg);
      throw new IllegalArgumentException(msg);
    }

    if (componentTypeLength < 0) {
      // This is a reference to a previously seen class
      int offset = ~componentTypeLength; // Decode the reference using bitwise complement
      Class<?> referencedClass = bufferOffset2Class.get(offset);

      if (referencedClass == null) {
        final var msg = "Invalid class reference offset: " + offset;
        LOGGER.severe(() -> msg);
        throw new IllegalArgumentException(msg);
      }
      return referencedClass;
    } else {
      // This is a new class name
      int currentPosition = buffer.position() - 2; // Position before reading the length

      if (buffer.remaining() < componentTypeLength) {
        final var msg = "Buffer underflow: needed " + componentTypeLength +
            " bytes but only " + buffer.remaining() + " remaining";
        LOGGER.severe(() -> msg);
        throw new IllegalArgumentException(msg);
      }

      // Read the class name
      byte[] classNameBytes = new byte[componentTypeLength];
      buffer.get(classNameBytes);
      String className = new String(classNameBytes, UTF_8);

      // Validate class name - add basic validation that allows array type names like `[I`, `[[I`, `[L`java.lang.String;` etc.
      if (!className.matches("[\\[\\]a-zA-Z0-9_.$;]+")) {
        final var msg = "Invalid class name format: " + className;
        LOGGER.severe(() -> msg);
        throw new IllegalArgumentException(msg);
      }

      // Load the class using our helper method
      Class<?> loadedClass = getClassForName(className);

      // Store in our map for future references
      bufferOffset2Class.put(currentPosition, loadedClass);

      return loadedClass;
    }
  }

  /// Helper method to get an enum constant with proper type witness
  ///
  /// @param enumClass The enum class
  /// @param enumName The name of the enum constant
  /// @return The enum constant
  @SuppressWarnings("unchecked")
  public static <E extends Enum<E>> Object enumValueOf(Class<?> enumClass, String enumName) {
    return Enum.valueOf((Class<E>) enumClass, enumName);
  }

  /// Class.forName cannot handle primitive types directly, so we need to map them to their wrapper classes.
  static Class<?> getClassForName(String name) throws ClassNotFoundException {
    // Handle primitive types which can't be loaded directly with Class.forName
    return switch (name) {
      case "boolean" -> BOOLEAN._class();
      case "byte" -> BYTE._class();
      case "char" -> CHARACTER._class();
      case "short" -> SHORT._class();
      case "int" -> INTEGER._class();
      case "long" -> LONG._class();
      case "float" -> FLOAT._class();
      case "double" -> DOUBLE._class();
      default -> Class.forName(name);
    };
  }

  static <R extends Record> Pickler<R> manufactureRecordPickler(Class<R> recordClass) {
    final Map<Integer, MethodHandle> fallbackConstructorHandles = new HashMap<>();
    final MethodHandle[] componentAccessors;
    final int canonicalParamCount;
    final MethodHandles.Lookup lookup = MethodHandles.lookup();
    MethodHandle canonicalConstructorHandle;
    try {
      RecordComponent[] components = recordClass.getRecordComponents();
      Optional
          .ofNullable(components)
          .orElseThrow(() ->
              new IllegalArgumentException(recordClass.getName() + " is not actually a concrete record class. You may have tried to use a Record[]."));
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

    final int componentCount;

    // Get the canonical constructor and any fallback constructors for schema evolution
    try {
      final RecordComponent[] components = recordClass.getRecordComponents();
      componentCount = components.length;
      // Extract component types for the canonical constructor
      final Class<?>[] canonicalParamTypes = Arrays.stream(components)
          .map(RecordComponent::getType)
          .toArray(Class<?>[]::new);
      canonicalParamCount = canonicalParamTypes.length;

      // Get all public constructors
      final Constructor<?>[] allConstructors = recordClass.getConstructors();

      // Find the canonical constructor and potential fallback constructors
      canonicalConstructorHandle = null;

      for (Constructor<?> constructor : allConstructors) {
        Class<?>[] currentParamTypes = constructor.getParameterTypes();
        int currentParamCount = constructor.getParameterCount();
        MethodHandle handle;

        try {
          handle = lookup.unreflectConstructor(constructor);
        } catch (IllegalAccessException e) {
          LOGGER.warning("Cannot access constructor with " + currentParamCount +
              " parameters for " + recordClass.getName() + ": " + e.getMessage());
          continue;
        }

        if (Arrays.equals(currentParamTypes, canonicalParamTypes)) {
          // Found the canonical constructor
          canonicalConstructorHandle = handle;
        } else {
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
      }

      // If we didn't find the canonical constructor, try to find it directly
      if (canonicalConstructorHandle == null) {
        try {
          // Create method type for the canonical constructor
          MethodType constructorType = MethodType.methodType(void.class, canonicalParamTypes);
          canonicalConstructorHandle = lookup.findConstructor(recordClass, constructorType);
        } catch (NoSuchMethodException | IllegalAccessException e) {
          final var msg = "Failed to access canonical constructor for record '" +
              recordClass.getName() + "' due to " + e.getClass().getSimpleName();
          LOGGER.severe(() -> msg);
          throw new IllegalArgumentException(msg, e);
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
      void serializeWithMap(Map<Class<?>, Integer> classToOffset, Work buffer, R object) {
        final var components = components(object);
        // Write the number of components as an unsigned byte (max 255)
        buffer.putInt(components.length);
        Arrays.stream(components).forEach(c -> Companion.write(classToOffset, buffer, c));
      }

      @Override
      R deserializeWithMap(Work buffer, Map<Integer, Class<?>> bufferOffset2Class) {
        // Read the number of components as an unsigned byte
        final int length = buffer.getInt();
        Compatibility.validate(compatibility, recordClassName, componentCount, length);
        // This may unload from the stream things that we will ignore
        final Object[] components = new Object[length];
        Arrays.setAll(components, ignored -> deserializeValue(bufferOffset2Class, buffer));
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

          if (numComponents == canonicalParamCount) {
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
        serializeWithMap(new HashMap<>(), Work.of(buffer), object);
      }

      @Override
      public R deserialize(ByteBuffer buffer) {
        buffer.order(java.nio.ByteOrder.BIG_ENDIAN);
        return deserializeWithMap(Work.of(buffer), new HashMap<>());
      }

      /// This recursively descends through the object graph to find the size of the object
      /// It has its own logic to count jup the sizes that needs seperate handling
      @Override
      public int sizeOf(R object) {
        Work work = Work.of(null);
        serializeWithMap(new HashMap<>(), work, object);
        return work.size();
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
      public void serialize(S object, ByteBuffer buffer) {
        buffer.order(java.nio.ByteOrder.BIG_ENDIAN);
        Work work = Work.of(buffer);
        if (object == null) {
          work.put(NULL.marker());
          return;
        }

        // Cast the sealed interface to the concrete type.
        @SuppressWarnings("unchecked") Class<? extends S> concreteType = (Class<? extends S>) object.getClass();

        // write the type identifier
        writeDeduplicatedClassName(work, concreteType, new HashMap<>(), shortNames.get(concreteType));

        // Delegate to subtype pickler
        Pickler<? extends S> pickler = subPicklers.get(concreteType);
        //noinspection unchecked
        ((RecordPickler<Record>) pickler).serializeWithMap(new HashMap<>(), work, (Record) object);
      }

      @Override
      public S deserialize(ByteBuffer buffer2) {
        buffer2.order(java.nio.ByteOrder.BIG_ENDIAN);
        // if the type is NULL, return null, else read the type identifier
        buffer2.mark();
        if (buffer2.get() == NULL.marker()) {
          return null;
        }
        buffer2.reset();
        Work buffer = Work.of(buffer2);
        // Read type identifier
        Class<? extends S> concreteType = resolveCachedClassByPickedName(buffer);
        // Get subtype pickler
        RecordPickler<?> pickler = (RecordPickler<?>) subPicklers.get(concreteType);
        //noinspection unchecked
        return (S) pickler.deserializeWithMap(buffer, new HashMap<>());
      }

      @Override
      public int sizeOf(S object) {
        if (object == null) {
          return 1; // Size of NULL marker
        }
        // Use a dry run buffer that counts up what would be written
        Work work = Work.of();
        // Cast the sealed interface to the concrete type.
        @SuppressWarnings("unchecked") Class<? extends S> concreteType = (Class<? extends S>) object.getClass();
        // write the type identifier
        writeDeduplicatedClassName(work, concreteType, new HashMap<>(), shortNames.get(concreteType));
        // Delegate to subtype pickler
        Pickler<? extends S> pickler = subPicklers.get(concreteType);
        //noinspection unchecked
        ((RecordPickler<Record>) pickler).serializeWithMap(new HashMap<>(), work, (Record) object);
        return work.size();
      }

      @Override
      Class<? extends S> resolveCachedClassByPickedName(Work buffer) {
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
