package io.github.simbo1905.no.framework;

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

public class Companion {

  public static final ConcurrentHashMap<Class<?>, Pickler<?>> REGISTRY = new ConcurrentHashMap<>();

  // In Pickler interface
  @SuppressWarnings("unchecked")
  static <T> Pickler<T> getOrCreate(Class<T> type, Supplier<Pickler<T>> supplier) {
    return (Pickler<T>) REGISTRY.computeIfAbsent(type, k -> supplier.get());
  }

  static <R extends Record> Pickler<R> manufactureRecordPickler(Class<R> recordClass) {

    return new RecordPickler<>(recordClass) {
      @Override
      public void serialize(R object, ByteBuffer buffer) {
        buffer.order(java.nio.ByteOrder.BIG_ENDIAN);
        WriteOperations operations = new WriteOperations(new HashMap<>(), buffer);
        serializeWithMap(operations, object);
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
      public void serialize(S object, ByteBuffer buffer) {
        if (object == null) {
          buffer.put(NULL.marker());
          return;
        }
        buffer.order(java.nio.ByteOrder.BIG_ENDIAN);
        WriteOperations operations = new WriteOperations(new HashMap<>(), buffer);
        // Cast the sealed interface to the concrete type.
        @SuppressWarnings("unchecked") Class<? extends S> concreteType = (Class<? extends S>) object.getClass();

        final Map<Class<?>, Integer> classToOffset = new HashMap<>();

        // write the type identifier
        writeDeduplicatedClassName(buffer, concreteType, classToOffset, shortNames.get(concreteType));

        // Delegate to subtype pickler
        Pickler<? extends S> pickler = subPicklers.get(concreteType);
        //noinspection unchecked
        ((RecordPickler<Record>) pickler).serializeWithMap(operations, (Record) object);
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
      case Optional<?> ignored -> OPTIONAL_EMPTY.marker();
      case Type ignored -> TYPE.marker();

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

/// The  encoding used here diverges from the "original"
record Type(String name) {
}

record TypeOffset(int offset) {
}
