// SPDX-FileCopyrightText: 2025 Simon Massey
// SPDX-License-Identifier: MIT

package io.github.simbo1905.no.framework;

import java.util.*;
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
  static <R extends Record> void serializeMany(R[] array, WriteBuffer buffer) {
    buffer.put(ARRAY.marker());
    buffer.putInt(array.length);

    @SuppressWarnings("unchecked") Pickler<R> pickler = Pickler.forRecord((Class<R>) array.getClass().getComponentType());
    Arrays.stream(array).forEach(element -> pickler.serialize(buffer, element));
  }

  /// Unloads from the buffer a list of messages that were written using [#serializeMany].
  /// By default, there must be an exact match between the class name of the record and the class name in the buffer.
  static <R extends Record> List<R> deserializeMany(Class<R> componentType, ReadBuffer buffer) {
    Objects.requireNonNull(componentType);
    Objects.requireNonNull(buffer);
    if (buffer.isClosed()) {
      throw new IllegalStateException("Buffer is closed");
    }
    final var readBuffer = ((ReadBufferImpl) buffer).buffer;
    byte marker = readBuffer.get();
    if (marker != ARRAY.marker()) throw new IllegalArgumentException("Invalid array marker");

    return IntStream.range(0, readBuffer.getInt())
        .mapToObj(i -> Pickler.forRecord(componentType).deserialize(buffer))
        .toList();
  }

  /// Recursively loads the components reachable through record into the buffer into the [WriteBuffer].
  /// A packed buffer is a wrapper around a byte buffer that tracks the position of class names or
  /// enum strings to deduplicate them. Once the buffer is flipped it is no longer usable. You should
  /// therefore wrap it in a try-with-resources statement which will close it. After that you may flip
  /// it to read it entirely.
  ///
  /// @param buffer The buffer to write into
  /// @param record The record to serialize
  /// @return The number of bytes written to the buffer
  int serialize(WriteBuffer buffer, T record);

  /// Recursively unloads components from the buffer and invokes the record constructor.
  /// @param buffer The buffer to read from
  /// @return The deserialized record
  T deserialize(ReadBuffer buffer);

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
  InternedName {
    Objects.requireNonNull(name);
  }
}

record InternedOffset(int offset) {
  InternedOffset {
    if (offset >= 0) {
      throw new IllegalArgumentException("Offset must be negative");
    }
  }
}

record InternedPosition(int position) {
  InternedPosition {
    if (position < 0) {
      throw new IllegalArgumentException("Position must be non-negative");
    }
  }

  Object offset(int position) {
    return new InternedOffset(position() - position);
  }
}
