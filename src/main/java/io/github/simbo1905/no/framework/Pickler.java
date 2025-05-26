// SPDX-FileCopyrightText: 2025 Simon Massey
// SPDX-License-Identifier: MIT

package io.github.simbo1905.no.framework;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static io.github.simbo1905.no.framework.Companion.manufactureRecordPickler;

public interface Pickler<T> {
  java.util.logging.Logger LOGGER = java.util.logging.Logger.getLogger(Pickler.class.getName());

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

  /// Allocates a buffer that is large enough to hold the serialized record. This method does not
  WriteBuffer allocateSufficient(T record);
}

record InternedName(String name) {
  InternedName {
    Objects.requireNonNull(name);
    // TODO shorten java.lang.String to be j.l.String the StringBuild reverse it
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
