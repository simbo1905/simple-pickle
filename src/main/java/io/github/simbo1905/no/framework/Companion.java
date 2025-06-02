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
/**
 * A companion class providing utility methods for working with records and their components.
 * This class is not intended to be instantiated directly.
 */
class Companion {
  /// We cache the picklers for each class to avoid creating them multiple times
  static ConcurrentHashMap<Class<?>, Pickler<?>> REGISTRY = new ConcurrentHashMap<>();

  ///  Here we are typing things as `Record` to avoid the need for a cast
  @SuppressWarnings("unchecked")
  static <R extends Record> RecordPickler<R> manufactureRecordPickler(final Class<R> recordClass) {
    Objects.requireNonNull(recordClass);
    // Check registry first to prevent infinite recursion

    // Phase 1: Create main pickler (analysis only) and add to registry
    final var result = new RecordPickler<>(recordClass);

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
        RecordPickler<? extends Record> delegatePickler = manufactureRecordPickler(recordType);
        //pickler.delegatePicklers.put(discoveredType, delegatePickler);
      } else {
        LOGGER.info("Using existing pickler from registry for: " + discoveredType.getName());
        pickler.delegatePicklers.put(discoveredType, REGISTRY.get(discoveredType));
      }
    }
  }

  /// Implementation that traverses the hierarchy using a visited set to avoid cycles.
  /// This is java doc comments
  ///
  ///
  /// @param current the current class being processed
  /// @param visited set of already visited classes
  /// @return stream of classes in the hierarchy
  /// @throws NullPointerException if current is null
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
                .flatMap(component -> {
                  LOGGER.finer(() -> "Analyzing component " + component.getName() + " with type " + component.getGenericType());
                  try {
                    TypeStructure structure = TypeStructure.analyze(component.getGenericType());
                    LOGGER.finer(() -> "Component " + component.getName() + " discovered types: " + structure.types().stream().map(Class::getSimpleName).toList());
                    return structure.types().stream();
                  } catch (Exception e) {
                    LOGGER.finer(() -> "Failed to analyze component " + component.getName() + ": " + e.getMessage());
                    return Stream.of(component.getType()); // Fallback to direct type
                  }
                })
                .filter(t -> t.isRecord() || t.isSealed() || t.isEnum())
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



}
