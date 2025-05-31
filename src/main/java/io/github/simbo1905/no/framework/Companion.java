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

import static io.github.simbo1905.no.framework.Pickler.LOGGER;

class Companion {
  /// We cache the picklers for each class to avoid creating them multiple times
  static ConcurrentHashMap<Class<?>, Pickler<?>> REGISTRY = new ConcurrentHashMap<>();

  ///  Here we are typing things as `Record` to avoid the need for a cast
  @SuppressWarnings("unchecked")
  static <R extends Record> Pickler<R> manufactureRecordPickler(final Class<R> recordClass) {
    Objects.requireNonNull(recordClass);
    // Check registry first to prevent infinite recursion
    Pickler<?> existing = REGISTRY.get(recordClass);
    if (existing != null) {
      return (Pickler<R>) existing;
    }
    
    // Phase 1: Create main pickler (analysis only) and add to registry
    final var result = new RecordPickler<>(recordClass);
    REGISTRY.putIfAbsent(recordClass, result);
    
    // Phase 2: Create delegate picklers for discovered nested types
    populateDelegatePicklers(result);
    
    return result;
  }
  
  /// Create delegate picklers for all discovered nested types
  @SuppressWarnings("unchecked")
  private static void populateDelegatePicklers(RecordPickler<?> pickler) {
    for (Class<?> discoveredType : pickler.reflection.discoveredRecordTypes()) {
      if (!REGISTRY.containsKey(discoveredType)) {
        LOGGER.info("Creating delegate pickler for nested record: " + discoveredType.getName());
        Class<? extends Record> recordType = (Class<? extends Record>) discoveredType;
        Pickler<?> delegatePickler = manufactureRecordPickler(recordType);
        pickler.delegatePicklers.put(discoveredType, delegatePickler);
      } else {
        LOGGER.info("Using existing pickler from registry for: " + discoveredType.getName());
        pickler.delegatePicklers.put(discoveredType, REGISTRY.get(discoveredType));
      }
    }
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
