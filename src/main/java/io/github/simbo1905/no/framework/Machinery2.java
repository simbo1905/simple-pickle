// SPDX-FileCopyrightText: 2025 Simon Massey
// SPDX-License-Identifier: MIT

package io.github.simbo1905.no.framework;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.RecordComponent;
import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static io.github.simbo1905.no.framework.Pickler.LOGGER;

/// Unified machinery for discovering all reachable types and building array-based serialization logic.
/// This replaces the fragmented RecordPickler/SealedPickler approach with a single unified system.
class Machinery2 {

    /// Complete type analysis result containing all discovered user types and their metadata
    record UnifiedTypeAnalysis(
        Class<?>[] discoveredClasses,     // Lexicographically sorted user types
        Tag[] tags,                       // Corresponding type tags (RECORD, ENUM, etc.)
        MethodHandle[] constructors,      // Sparse array, only populated for records
        MethodHandle[][] componentAccessors, // Sparse array of accessor arrays for records
        BiConsumer<WriteBuffer, Object>[] writers,   // Component writers by index
        Function<ReadBuffer, Object>[] readers,      // Component readers by index
        Map<Class<?>, Integer> classToOrdinal       // Fast lookup: class -> array index
    ) {
        /// Get the ordinal (array index) for a given user type class
        int getOrdinal(Class<?> clazz) {
            Integer ordinal = classToOrdinal.get(clazz);
            if (ordinal == null) {
                throw new IllegalArgumentException("Unknown user type: " + clazz.getName());
            }
            return ordinal;
        }
    }

    /// Perform exhaustive type discovery and analysis for a given root class.
    /// Combines Companion.recordClassHierarchy() + TypeStructure.analyze() to find ALL reachable types.
    static UnifiedTypeAnalysis analyzeAllReachableTypes(Class<?> rootClass) {
        LOGGER.info(() -> "Starting unified type analysis for root class: " + rootClass.getName());

        // Phase 1: Discover all reachable user types using existing recordClassHierarchy
        Set<Class<?>> allReachableClasses = Companion.recordClassHierarchy(rootClass, new HashSet<>())
            .filter(clazz -> clazz.isRecord() || clazz.isEnum() || clazz.isSealed())
            .collect(Collectors.toSet());

        LOGGER.info(() -> "Discovered " + allReachableClasses.size() + " reachable user types: " +
            allReachableClasses.stream().map(Class::getSimpleName).toList());

        // Phase 2: Filter out sealed interfaces (keep only concrete records and enums for serialization)
        Class<?>[] discoveredClasses = allReachableClasses.stream()
            .filter(clazz -> !clazz.isSealed()) // Remove sealed interfaces - they're only for discovery
            .sorted(Comparator.comparing(Class::getName))
            .toArray(Class<?>[]::new);

        LOGGER.info(() -> "Filtered to " + discoveredClasses.length + " concrete types (removed sealed interfaces)");

        LOGGER.fine(() -> "Lexicographically sorted classes: " +
            Arrays.stream(discoveredClasses).map(Class::getSimpleName).toList());

        // Phase 3: Create class-to-ordinal lookup map
        Map<Class<?>, Integer> classToOrdinal = new HashMap<>();
        IntStream.range(0, discoveredClasses.length).forEach(i ->
            classToOrdinal.put(discoveredClasses[i], i)
        );

        // Phase 4: Analyze each type and build corresponding metadata arrays
        Tag[] tags = new Tag[discoveredClasses.length];
        MethodHandle[] constructors = new MethodHandle[discoveredClasses.length];
        MethodHandle[][] componentAccessors = new MethodHandle[discoveredClasses.length][];

        IntStream.range(0, discoveredClasses.length).forEach(i -> {
            final var clazz = discoveredClasses[i];
            final var index = i; // effectively final for lambda

            if (clazz.isRecord()) {
                tags[i] = Tag.RECORD;
                constructors[i] = getRecordConstructor(clazz);
                componentAccessors[i] = getRecordComponentAccessors(clazz);
                LOGGER.fine(() -> "Analyzed record " + clazz.getSimpleName() + " at index " + index +
                    " with " + componentAccessors[index].length + " components");
            } else if (clazz.isEnum()) {
                tags[i] = Tag.ENUM;
                // constructors[i] and componentAccessors[i] remain null for enums
                LOGGER.fine(() -> "Analyzed enum " + clazz.getSimpleName() + " at index " + index);
            } else {
                throw new IllegalStateException("Unexpected type in filtered array: " + clazz.getName() + " (should be record or enum only)");
            }
        });

        // Phase 5: Build writers and readers arrays (placeholder for now)
        @SuppressWarnings({"unchecked"})
        BiConsumer<WriteBuffer, Object>[] writers = new BiConsumer[discoveredClasses.length];
        @SuppressWarnings({"unchecked"})
        Function<ReadBuffer, Object>[] readers = new Function[discoveredClasses.length];

        // TODO: Build actual writers and readers using existing TypeStructure.analyze logic
        LOGGER.info(() -> "TODO: Build writers and readers arrays for " + discoveredClasses.length + " types");

        return new UnifiedTypeAnalysis(
            discoveredClasses,
            tags,
            constructors,
            componentAccessors,
            writers,
            readers,
            classToOrdinal
        );
    }

    /// Get the canonical constructor method handle for a record class
    private static MethodHandle getRecordConstructor(Class<?> recordClass) {
        try {
            RecordComponent[] components = recordClass.getRecordComponents();
            Class<?>[] parameterTypes = Arrays.stream(components)
                .map(RecordComponent::getType)
                .toArray(Class<?>[]::new);

            var constructor = recordClass.getDeclaredConstructor(parameterTypes);
            return MethodHandles.lookup().unreflectConstructor(constructor);
        } catch (Exception e) {
            throw new RuntimeException("Failed to get constructor for record: " + recordClass.getName(), e);
        }
    }

    /// Get method handles for all record component accessors
    private static MethodHandle[] getRecordComponentAccessors(Class<?> recordClass) {
        try {
            RecordComponent[] components = recordClass.getRecordComponents();
            MethodHandle[] accessors = new MethodHandle[components.length];

            IntStream.range(0, components.length).forEach(i -> {
                try {
                    accessors[i] = MethodHandles.lookup().unreflect(components[i].getAccessor());
                } catch (IllegalAccessException e) {
                    // Wrap in a RuntimeException to be caught by the outer catch block
                    // This is necessary because forEach lambda can't directly throw checked exceptions
                    // that are not declared by the functional interface's method.
                    throw new RuntimeException("Failed to unreflect accessor for component " + components[i].getName() + " in record " + recordClass.getName(), e);
                }
            });

            return accessors;
        } catch (Exception e) {
            // This will catch RuntimeExceptions thrown from the lambda as well
            throw new RuntimeException("Failed to get accessors for record: " + recordClass.getName(), e);
        }
    }

}
