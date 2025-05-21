package io.github.simbo1905.no.framework;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static io.github.simbo1905.no.framework.Companion.manufactureRecordPickler;
import static io.github.simbo1905.no.framework.Constants.ARRAY;

public interface Pickler<T> {
  java.util.logging.Logger LOGGER = java.util.logging.Logger.getLogger(Pickler.class.getName());

  /// A convenient helper to serializes an array of records. Due to Java's runtime type erasure you must use
  /// explicitly declared arrays and not have any compiler warnings about possible misalignment of types. Use
  /// [Pickler#deserializeMany(java.lang.Class, java.nio.ByteBuffer)] for deserialization.
  ///
  /// WARNING: Do not attempt to use helper methods on Java collections into convert Collections into arrays!
  ///
  /// The boring safe way to make an array from a list of records is to do an explicit shallow copy into an explicitly
  /// instantiated array:
  ///
  /// ```
  /// People[] people = new People[peopleList.size()]
  /// Arrays.setAll(people, i -> peopleList.get(i));
  ///```
  ///
  /// The shallow copy is the price to pay for a type safe way to serialization many things.
  /// See the `README.md` for a discussion on how to void the shallow copy.
  ///
  /// @param array The array to serialize
  /// @param buffer The buffer to write into
  static <R extends Record> void serializeMany(R[] array, PackedBuffer buffer) {
    buffer.put(ARRAY.marker());
    buffer.putInt(array.length);

    @SuppressWarnings("unchecked") Pickler<R> pickler = Pickler.forRecord((Class<R>) array.getClass().getComponentType());
    Arrays.stream(array).forEach(element -> pickler.serialize(buffer, element));
  }

  /// Unloads from the buffer a list of messages that were written using [#serializeMany].
  /// By default, there must be an exact match between the class name of the record and the class name in the buffer.
  static <R extends Record> List<R> deserializeMany(Class<R> componentType, ByteBuffer buffer) {
    byte marker = buffer.get();
    if (marker != ARRAY.marker()) throw new IllegalArgumentException("Invalid array marker");

    return IntStream.range(0, buffer.getInt())
        .mapToObj(i -> Pickler.forRecord(componentType).deserialize(buffer))
        .toList();
  }

  static <A> int sizeOfMany(A[] array) {
    throw new AssertionError("not implemented");
  }

  /// PackedBuffer is an auto-closeable wrapper around ByteBuffer that tracks the written position of record class names
  /// You should use a try-with-resources block to ensure that it is closed once you have
  /// written a set of records into it. You also cannot use it safely after you have:
  /// - flipped the buffer
  /// - read from the buffer
  default PackedBuffer wrap(ByteBuffer buf) {
    return new PackedBufferImpl(buf);
  }

  /// PackedBuffer is an auto-closeable wrapper around ByteBuffer that tracks the written position of record class names
  /// You should use a try-with-resources block to ensure that it is closed once you have
  /// written a set of records into it. You also cannot use it safely after you have:
  /// - flipped the buffer
  /// - read from the buffer
  static PackedBuffer allocate(int size) {
    return new PackedBufferImpl(ByteBuffer.allocate(size));
  }

  /// This method allocates a buffer of sufficient size to hold the serialized form of the record.
  /// It makes a worst case estimate that strings are UTF-8 encoded using 3 bytes and that no
  /// compression of long or int or class names is possible. The actual size will likely be a lot smaller.
  /// PackedBuffer is an auto-closeable wrapper around ByteBuffer that tracks the written position of record class names
  /// You should use a try-with-resources block to ensure that it is closed once you have
  /// written a set of records into it. You also cannot use it safely after you have:
  /// - flipped the buffer
  /// - read from the buffer
  default PackedBuffer allocateSufficient(T originalRoot) {
    return null;
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
    final Class<?>[] subclasses = Companion.recordClassHierarchy(sealedClass, new HashSet<>()).toArray(Class<?>[]::new);

    LOGGER.fine(Stream.of(sealedClass).map(Object::toString).collect(Collectors.joining(",")) + " subclasses: " +
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

    @SuppressWarnings("unchecked") final Map<String, Class<?>> classesByShortName =
        Arrays.stream(subclasses)
            .map(cls -> (Class<? extends Record>) cls) // Safe due to validateSealedRecordHierarchy
        .collect(Collectors.toMap(
            cls -> cls.getName().substring(commonPrefixLength),
                cls -> cls
            )
        );

    @SuppressWarnings("unchecked")
    Map<Class<? extends S>, Pickler<? extends S>> picklersByClass = classesByShortName.entrySet().stream()
        .filter(e -> e.getValue().isRecord())
        .collect(Collectors.toMap(
            e -> (Class<? extends S>) e.getValue(),
            e -> {
              // Double cast required to satisfy compiler
              @SuppressWarnings("unchecked")
              Class<? extends Record> recordCls = (Class<? extends Record>) e.getValue();
              return (Pickler<S>) manufactureRecordPickler(classesByShortName, recordCls, e.getKey());
            }
        ));

    final var sealedPickler = new SealedPickler<>(picklersByClass, classesByShortName);
    Companion.REGISTRY.putIfAbsent(sealedClass, sealedPickler);
    //noinspection unchecked
    return (Pickler<S>) Companion.REGISTRY.get(sealedClass);
  }
}

record InternedName(String name) {
}

record InternedOffset(int offset) {
}

record InternedPosition(int position) {
  public Object offset(int position) {
    return new InternedOffset(position() - position);
  }
}
