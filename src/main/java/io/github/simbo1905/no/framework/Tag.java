// SPDX-FileCopyrightText: 2025 Simon Massey
// SPDX-License-Identifier: MIT

package io.github.simbo1905.no.framework;

import java.util.*;

/// Type tag enum for identifying different data types in the serialization protocol
enum Tag {
  // Primitive types
  BOOLEAN(boolean.class, Boolean.class),
  BYTE(byte.class, Byte.class),
  SHORT(short.class, Short.class),
  CHARACTER(char.class, Character.class),
  INTEGER(int.class, Integer.class),
  LONG(long.class, Long.class),
  FLOAT(float.class, Float.class),
  DOUBLE(double.class, Double.class),
  STRING(String.class),

  // Container types
  OPTIONAL(Optional.class),
  LIST(List.class),
  MAP(Map.class),

  // Complex types
  ENUM(Enum.class), //
  ARRAY(Arrays.class), // Arrays don't have a single class use Arrays.class as a marker
  RECORD(Record.class), // FIXME TODO this has been used as a placeholder for enum or record as could be a sealed interface rename to USER
  UUID(java.util.UUID.class)
  ;

  final Class<?>[] supportedClasses;

  Tag(Class<?>... classes) {
    Objects.requireNonNull(classes);
    this.supportedClasses = classes;
  }

  static Tag fromClass(Class<?> clazz) {
    for (Tag tag : values()) {
      for (Class<?> supported : tag.supportedClasses) {
        if (supported.equals(clazz) || supported.isAssignableFrom(clazz)) {
          return tag;
        }
      }
    }
    // Special case for arrays and records
    if (clazz.isArray()) return ARRAY;
    if (clazz.isRecord()) return RECORD;
    // For user types (including sealed interfaces), we'll use RECORD tag as a placeholder
    // The actual type will be determined at runtime
    if (clazz.isInterface() && clazz.isSealed()) return RECORD;
    throw new IllegalArgumentException("Unsupported class: " + clazz.getName());
  }
}
