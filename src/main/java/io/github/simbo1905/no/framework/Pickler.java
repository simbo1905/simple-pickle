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

  static void serializeMany(Object[] dogs, PackedBuffer buffer) {
    throw new AssertionError("not implemented");
  }

  static List<Object> deserializeMany(Class<?> dogClass, ByteBuffer buffer) {
    throw new AssertionError("not implemented");
  }

  static int sizeOfMany(Object[] array) {
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
    return Companion.getOrCreate(recordClass, () -> manufactureRecordPickler(recordClass));
  }

  static <S> Pickler<S> forSealedInterface(Class<S> sealedClass) {
    // Get all permitted record subclasses. This will throw an exception if the class is not sealed or if any of the subclasses are not records or sealed interfaces.
    final Class<?>[] subclasses = Companion.validateSealedRecordHierarchy(sealedClass).toArray(Class<?>[]::new);

    // The following computes the shortest set record names when all the common prefixes are removed when also including the name of the sealed interface itself
    @SuppressWarnings("unchecked") final Map<String, Class<? extends S>> classesByShortName = Stream.concat(Stream.of(sealedClass), Arrays.stream(subclasses))
        .map(cls -> (Class<? extends S>) cls) // Safe due to validateSealedRecordHierarchy
        .collect(Collectors.toMap(
                cls -> cls.getName().substring(
                    Arrays.stream(subclasses)
                        .map(Class::getName)
                        .reduce((a, b) ->
                            !a.isEmpty() && !b.isEmpty() ?
                                a.substring(0,
                                    IntStream.range(0, Math.min(a.length(), b.length()))
                                        .filter(i -> a.charAt(i) != b.charAt(i))
                                        .findFirst()
                                        .orElse(Math.min(a.length(), b.length()))) : "")
                        .orElse("").length()),
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
    return new SealedPickler<>(picklersByClass, classesByShortName);
  }

  int sizeOf(Object record);
}

