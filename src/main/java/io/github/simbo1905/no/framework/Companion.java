// SPDX-FileCopyrightText: 2025 Simon Massey
// SPDX-License-Identifier: MIT

package io.github.simbo1905.no.framework;

import java.lang.reflect.RecordComponent;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

class Companion {
  /// We cache the picklers for each class to avoid creating them multiple times
  static ConcurrentHashMap<Class<?>, Pickler<?>> REGISTRY = new ConcurrentHashMap<>();

  ///  Here we are typing things as `Record` to avoid the need for a cast
  static <R extends Record> Pickler<R> manufactureRecordPickler(final Class<R> recordClass) {
    Objects.requireNonNull(recordClass);
    final var result = new RecordPickler<>(recordClass);
    REGISTRY.putIfAbsent(recordClass, result);
    return result;
  }

  /// Implementation that traverses the hierarchy using a visited set to avoid cycles.
  ///
  /// @param current the current class being processed
  /// @param visited set of already visited classes
  /// @return stream of classes in the hierarchy
  static Stream<Class<?>> recordClassHierarchy(
      final Class<?> current,
      final Set<Class<?>> visited
  ) {
    if (!visited.add(current)) {
      return Stream.empty();
    }

    return Stream.concat(
        Stream.of(current),
        Stream.concat(
            current.isSealed()
                ? Arrays.stream(current.getPermittedSubclasses())
                : Stream.empty(),

            current.isRecord()
                ? Arrays.stream(current.getRecordComponents())
                .map(RecordComponent::getType)
                .filter(t -> t.isRecord() || t.isSealed())
                : Stream.empty()
        ).flatMap(child -> recordClassHierarchy(child, visited))
    );
  }

  static Map<String, Class<?>> nameToBasicClass = Map.of(
      "byte", byte.class,
      "short", short.class,
      "char", char.class,
      "int", int.class,
      "long", long.class,
      "float", float.class,
      "double", double.class
  );

  /// This method cannot be inlined as it is required as a type witness to allow the compiler to downcast the pickler
  @SuppressWarnings({"unchecked", "rawtypes"})
  static void serializeWithPickler(WriteBufferImpl buf, Pickler<?> pickler, Object object) {
    ((RecordPickler) pickler).serialize(buf, (Record) object);
  }

}
