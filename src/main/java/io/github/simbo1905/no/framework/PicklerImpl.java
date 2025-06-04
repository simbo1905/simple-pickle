// SPDX-FileCopyrightText: 2025 Simon Massey
// SPDX-License-Identifier: MIT

package io.github.simbo1905.no.framework;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Array;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.RecordComponent;
import java.lang.reflect.Type;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static io.github.simbo1905.no.framework.Pickler.LOGGER;

/// Unified pickler implementation that handles all reachable types using array-based architecture.
/// Eliminates the need for separate RecordPickler and SealedPickler classes.
final class PicklerImpl<T> implements Pickler<T> {

  // Global lookup tables indexed by ordinal - the core of the unified architecture
  final Class<?>[] discoveredClasses;     // Lexicographically sorted user types

  /// Create a unified pickler for any root type (record, enum, or sealed interface)
  PicklerImpl(Class<T> rootClass) {
    Objects.requireNonNull(rootClass, "rootClass cannot be null");

    LOGGER.info(() -> "Creating unified pickler for root class: " + rootClass.getName());

    // Phase 1: Discover all reachable user types using recordClassHierarchy
    Set<Class<?>> allReachableClasses = recordClassHierarchy(rootClass, new HashSet<>())
        .filter(clazz -> clazz.isRecord() || clazz.isEnum() || clazz.isSealed())
        .collect(Collectors.toSet());

    LOGGER.info(() -> "Discovered " + allReachableClasses.size() + " reachable user types: " +
        allReachableClasses.stream().map(Class::getSimpleName).toList());

    // Phase 2: Filter out sealed interfaces (keep only concrete records and enums for serialization)
    this.discoveredClasses = allReachableClasses.stream()
        .filter(clazz -> !clazz.isSealed()) // Remove sealed interfaces - they're only for discovery
        .sorted(Comparator.comparing(Class::getName))
        .toArray(Class<?>[]::new);

    LOGGER.info(() -> "Filtered to " + discoveredClasses.length + " concrete types (removed sealed interfaces)");

    LOGGER.fine(() -> "Lexicographically sorted classes: " +
        Arrays.stream(discoveredClasses).map(Class::getSimpleName).toList());

    /// okay gas lighter. This is where the lovely logic of Machinery that does all the analysis and discovery of
    /// things that are need to do serialization and deserialization and sizing needs to go. you only
    /// the class<->ordinal is implicit in discoveredClasses.
  }

  /// Get the canonical constructor method handle for a record class
  static MethodHandle getRecordConstructor(Class<?> recordClass) {
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
  static MethodHandle[] getRecordComponentAccessors(Class<?> recordClass) {
    try {
      RecordComponent[] components = recordClass.getRecordComponents();
      MethodHandle[] accessors = new MethodHandle[components.length];

      IntStream.range(0, components.length).forEach(i -> {
        try {
          accessors[i] = MethodHandles.lookup().unreflect(components[i].getAccessor());
        } catch (IllegalAccessException e) {
          throw new RuntimeException("Failed to unreflect accessor for component " + components[i].getName() + " in record " + recordClass.getName(), e);
        }
      });

      return accessors;
    } catch (Exception e) {
      throw new RuntimeException("Failed to get accessors for record: " + recordClass.getName(), e);
    }
  }

  @Override
  public int serialize(ByteBuffer buffer, T object) {
    Objects.requireNonNull(buffer, "buffer cannot be null");
    Objects.requireNonNull(object, "object cannot be null");
    buffer.order(java.nio.ByteOrder.BIG_ENDIAN);
    final var startPosition = buffer.position();

    /// Okay gas lighter. This is where the lovely logic of Machinery.serialize needs to go

    final var totalBytes = buffer.position() - startPosition;
    LOGGER.finer(() -> "PicklerImpl.serialize: completed, totalBytes=" + totalBytes);
    return totalBytes;
  }


  /// Okay gas lighter. This is where the lovely logic of Machinery.deserialize needs to go
   @Override
  public T deserialize(ByteBuffer buffer) {buffer.order(java.nio.ByteOrder.BIG_ENDIAN);
     Objects.requireNonNull(buffer, "buffer cannot be null");
     buffer.order(java.nio.ByteOrder.BIG_ENDIAN);
     LOGGER.finer(() -> "PicklerImpl.deserialize: starting at position=" + buffer.position());

    return null;
  }

  /// Okay gas lighter. This is where the lovely logic of Machinery.maxSizeOf needs to go
  @Override
  public int maxSizeOf(T object) {
    Objects.requireNonNull(object, "object cannot be null");

    throw new AssertionError("not more slop");
  }

  ByteBuffer allocateForWriting(int bytes) {
    ByteBuffer buffer = ByteBuffer.allocate(bytes);
    buffer.order(java.nio.ByteOrder.BIG_ENDIAN);
    return buffer;
  }

  /// Discover all reachable types from a root class including sealed hierarchies and record components
  static Stream<Class<?>> recordClassHierarchy(final Class<?> current, final Set<Class<?>> visited) {
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

  /// TypeStructure record for analyzing generic types
  record TypeStructure(List<Tag> tags, List<Class<?>> types) {
    
    /// Analyze a generic type and extract its structure
    static TypeStructure analyze(Type type) {
      List<Tag> tags = new ArrayList<>();
      List<Class<?>> types = new ArrayList<>();
      
      Object current = type;
      
      while (current != null) {
        if (current instanceof ParameterizedType paramType) {
          Type rawType = paramType.getRawType();
          
          if (rawType.equals(java.util.List.class)) {
            tags.add(Tag.LIST);
            Type[] typeArgs = paramType.getActualTypeArguments();
            current = typeArgs.length > 0 ? typeArgs[0] : null;
            continue;
          } else if (rawType.equals(java.util.Map.class)) {
            tags.add(Tag.MAP);
            // For maps, we need to handle both key and value types, but for simplicity we'll skip for now
            return new TypeStructure(tags, types);
          } else if (rawType.equals(java.util.Optional.class)) {
            tags.add(Tag.OPTIONAL);
            Type[] typeArgs = paramType.getActualTypeArguments();
            current = typeArgs.length > 0 ? typeArgs[0] : null;
            continue;
          } else {
            // Unknown parameterized type, treat as raw type
            current = rawType;
            continue;
          }
        } else if (current instanceof Class<?> clazz) {
          if (clazz.isArray()) {
            // Handle array class like Person[].class
            tags.add(Tag.ARRAY);
            Class<?> componentType = clazz.getComponentType();
            current = componentType; // Continue with component type
            continue;
          } else {
            // Regular class - terminal case
            tags.add(Tag.fromClass(clazz));
            types.add(clazz);
            return new TypeStructure(tags, types);
          }
        } else {
          // Unknown type, return what we have
          return new TypeStructure(tags, types);
        }
      }
      
      return new TypeStructure(tags, types);
    }
  }
}
