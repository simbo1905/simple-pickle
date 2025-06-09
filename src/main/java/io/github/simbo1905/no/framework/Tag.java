// SPDX-FileCopyrightText: 2025 Simon Massey
// SPDX-License-Identifier: MIT

package io.github.simbo1905.no.framework;

import java.util.*;
import java.util.stream.Stream;

/// Meta categorization of tags for cleaner switch expressions
enum MetaTag {
  VALUE,      // Value types that can be written directly
  CONTAINER,  // Container types that always delegate
  MIXED       // Could be either value or container at runtime
}

/// Type tag enum for identifying different data types in the serialization protocol
enum Tag {
  // Primitive types - all VALUE
  BOOLEAN(MetaTag.VALUE, boolean.class, Boolean.class),
  BYTE(MetaTag.VALUE, byte.class, Byte.class),
  SHORT(MetaTag.VALUE, short.class, Short.class),
  CHARACTER(MetaTag.VALUE, char.class, Character.class),
  INTEGER(MetaTag.VALUE, int.class, Integer.class),
  LONG(MetaTag.VALUE, long.class, Long.class),
  FLOAT(MetaTag.VALUE, float.class, Float.class),
  DOUBLE(MetaTag.VALUE, double.class, Double.class),
  STRING(MetaTag.VALUE, String.class),

  // Container types
  OPTIONAL(MetaTag.CONTAINER, Optional.class),
  LIST(MetaTag.CONTAINER, List.class),
  MAP(MetaTag.CONTAINER, Map.class),

  // Complex types
  ENUM(MetaTag.VALUE, Enum.class), // Enums are value types
  ARRAY(MetaTag.CONTAINER, Arrays.class), // Arrays delegate to element writers
  RECORD(MetaTag.CONTAINER, Record.class), // Records delegate to component writers
  INTERFACE(MetaTag.MIXED), // Sealed interfaces - handled specially in fromClass()
  UUID(MetaTag.VALUE, java.util.UUID.class)
  ;

  final MetaTag metaTag;
  final Class<?>[] supportedClasses;

  Tag(MetaTag metaTag, Class<?>... classes) {
    Objects.requireNonNull(metaTag);
    Objects.requireNonNull(classes);
    this.metaTag = metaTag;
    this.supportedClasses = classes;
  }
  
  public MetaTag metaTag() {
    return metaTag;
  }

  static Tag fromClass(Class<?> clazz) {
    // Check concrete types first
    if (clazz.isArray()) return ARRAY;
    if (clazz.isRecord()) return RECORD;
    if (clazz.isEnum()) return ENUM;
    
    // For sealed interfaces, do exhaustive analysis of entire hierarchy
    if (clazz.isInterface() && clazz.isSealed()) {
      // Use recordClassHierarchy to get all reachable types
      Set<Class<?>> allTypes = new HashSet<>();
      recordClassHierarchy(clazz, allTypes).forEach(c -> {}); // Collect all types
      
      // Check what concrete types exist in the hierarchy (excluding arrays and interfaces)
      boolean hasRecord = allTypes.stream()
          .filter(c -> !c.isArray() && !c.isInterface())
          .anyMatch(Class::isRecord);
      boolean hasEnum = allTypes.stream()
          .filter(c -> !c.isArray() && !c.isInterface())
          .anyMatch(Class::isEnum);
      
      // Return specific tag based on what we found
      if (hasRecord && hasEnum) return INTERFACE;  // Mixed permits
      if (hasRecord) return RECORD;  // Only records in hierarchy
      if (hasEnum) return ENUM;      // Only enums in hierarchy
    }
    
    // Check built-in types last
    return Arrays.stream(values())
        .filter(tag -> Optional.ofNullable(tag.supportedClasses)
            .stream()
            .flatMap(Arrays::stream)
            .anyMatch(supported -> supported.equals(clazz) || supported.isAssignableFrom(clazz)))
        .findFirst()
        .orElseThrow(() -> new IllegalArgumentException("Unsupported class: " + clazz.getName()));
  }
  
  /// Discover all reachable types from a class including sealed hierarchies
  /// (Copied from PicklerImpl to avoid circular dependency)
  private static Stream<Class<?>> recordClassHierarchy(final Class<?> current, final Set<Class<?>> visited) {
    if (!visited.add(current)) {
      return Stream.empty();
    }

    return Stream.concat(
        Stream.of(current),
        current.isSealed()
            ? Arrays.stream(current.getPermittedSubclasses())
                .flatMap(child -> recordClassHierarchy(child, visited))
            : Stream.empty()
    );
  }
}
