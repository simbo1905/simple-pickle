package io.github.simbo1905.no.framework;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static io.github.simbo1905.no.framework.Companion.manufactureRecordPickler;

public interface Pickler<T> {
  java.util.logging.Logger LOGGER = java.util.logging.Logger.getLogger(Pickler.class.getName());

  static void serializeMany(Object[] a, PackedBuffer b) {
    throw new AssertionError("not implemented");
  }

  static List<Object> deserializeMany(Class<?> a, ByteBuffer b) {
    throw new AssertionError("not implemented");
  }

  static int sizeOfMany(Object[] a) {
    throw new AssertionError("not implemented");
  }

  /// PackedBuffer is an auto-closeable wrapper around ByteBuffer that tracks the written position of record class names
  /// You should use a try-with-resources block to ensure that it is closed once you have
  /// written a set of records into it. You also cannot use it safely after you have:
  /// - flipped the buffer
  /// - read from the buffer
  default PackedBuffer wrap(ByteBuffer buf) {
    return new PackedBuffer(buf);
  }

  /// PackedBuffer is an auto-closeable wrapper around ByteBuffer that tracks the written position of record class names
  /// You should use a try-with-resources block to ensure that it is closed once you have
  /// written a set of records into it. You also cannot use it safely after you have:
  /// - flipped the buffer
  /// - read from the buffer
  static PackedBuffer allocate(int size) {
    return new PackedBuffer(ByteBuffer.allocate(size));
  }

  /// Recursively loads the components reachable through record into the buffer. It always writes out all the components.
  ///
  /// @param buffer The buffer to write into
  /// @param record The record to serialize
  void serialize(PackedBuffer buffer, T record);

  /// Recursively unloads components from the buffer and invokes a constructor following compatibility rules.
  /// @param buffer The buffer to read from
  /// @return The deserialized record
  T deserialize(ByteBuffer buffer);

  static <R extends Record> Pickler<R> forRecord(Class<R> recordClass) {
    // If we do computeIfAbsent they cannot get picklers for nested classes.
    final var pickler = manufactureRecordPickler(recordClass);
    Companion.REGISTRY.putIfAbsent(recordClass, pickler);
    //noinspection unchecked
    return (Pickler<R>) Companion.REGISTRY.get(recordClass);
  }

  static <S> Pickler<S> forSealedInterface(Class<S> sealedClass) {
    if (!sealedClass.isSealed()) {
      final var msg = "Class is not sealed: " + sealedClass.getName();
      LOGGER.severe(() -> msg);
      throw new IllegalArgumentException(msg);
    }
    // Get all permitted record subclasses. This will throw an exception if the class is not sealed or if any of the subclasses are not records or sealed interfaces.
    final Class<?>[] subclasses = Companion.recordClassHierarchy(sealedClass).toArray(Class<?>[]::new);

    LOGGER.info(Stream.of(sealedClass).map(Object::toString).collect(Collectors.joining(",")) + " subclasses: " +
        Stream.of(subclasses).map(Object::toString).collect(Collectors.joining(",\n")));

    final int commonPrefixLength = Stream.concat(Stream.of(sealedClass), Arrays.stream(subclasses))
        .map(Class::getName)
        .reduce((a, b) -> IntStream.range(0, Math.min(a.length(), b.length()))
            .filter(i -> a.charAt(i) != b.charAt(i))
            .findFirst()
            .stream()
            .mapToObj(i -> a.substring(0, i))
            .findFirst()
            .orElse(a.substring(0, Math.min(a.length(), b.length())))
        ).orElse("").length();

    @SuppressWarnings("unchecked") final Map<String, Class<? extends S>> classesByShortName =
        Arrays.stream(subclasses)
        .map(cls -> (Class<? extends S>) cls) // Safe due to validateSealedRecordHierarchy
        .collect(Collectors.toMap(
            cls -> cls.getName().substring(commonPrefixLength),
                cls -> cls
            )
        );

    @SuppressWarnings("unchecked")
    Map<Class<? extends S>, Pickler<? extends S>> picklersByClass = classesByShortName.entrySet().stream()
        .filter(e -> e.getValue().isRecord())
        .collect(Collectors.toMap(
            Map.Entry::getValue,
            e -> {
              // Double cast required to satisfy compiler
              @SuppressWarnings("unchecked")
              Class<? extends Record> recordCls = (Class<? extends Record>) e.getValue();
              return (Pickler<S>) manufactureRecordPickler(recordCls, e.getKey());
            }
        ));
    final var sealedPickler = new SealedPickler<>(picklersByClass, classesByShortName);
    Companion.REGISTRY.putIfAbsent(sealedClass, sealedPickler);
    //noinspection unchecked
    return (Pickler<S>) Companion.REGISTRY.get(sealedClass);
  }

  int sizeOf(Object record);
}

record InternedName(String name) {
}

record InternedOffset(int offset) {
}

record InternedPosition(int position) {
}
