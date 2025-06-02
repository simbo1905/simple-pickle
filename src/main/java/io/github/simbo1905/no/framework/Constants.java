// SPDX-FileCopyrightText: 2025 Simon Massey
// SPDX-License-Identifier: MIT

package io.github.simbo1905.no.framework;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/// Enum containing constants used throughout the Pickler implementation
enum Constants {
  NULL((byte) 1, 0, null),
  BOOLEAN((byte) 2, 1, boolean.class),
  BYTE((byte) 3, Byte.BYTES, byte.class),
  SHORT((byte) 4, Short.BYTES, short.class),
  CHARACTER((byte) 5, Character.BYTES, char.class),
  INTEGER((byte) 6, Integer.BYTES, int.class),
  INTEGER_VAR((byte) 7, Integer.BYTES, int.class),
  LONG((byte) 8, Long.BYTES, long.class),
  LONG_VAR((byte) 9, Long.BYTES, long.class),
  FLOAT((byte) 10, Float.BYTES, float.class),
  DOUBLE((byte) 11, Double.BYTES, double.class),
  STRING((byte) 12, 0, String.class),
  OPTIONAL_EMPTY((byte) 13, 0, Optional.class),
  OPTIONAL_OF((byte) 14, 0, Optional.class),
  ENUM((byte) 15, 0, Enum.class),
  ARRAY((byte) 16, 0, null),
  MAP((byte) 17, 0, Map.class),
  LIST((byte) 18, 0, List.class),
  RECORD((byte) 19, 0, Record.class),
  SAME_TYPE((byte) 20, 0, Record.class),
  UUID((byte) 21, 16, java.util.UUID.class);

  private final byte typeMarker;
  final int sizeInBytes;
  final Class<?> clazz;

  Constants(byte typeMarker, int sizeInBytes, Class<?> clazz) {
    this.typeMarker = typeMarker;
    this.sizeInBytes = sizeInBytes;
    this.clazz = clazz;
  }

  public byte marker() {
    return typeMarker;
  }

  public int wireMarker() {
    return -typeMarker;
  }

  public static Constants fromMarker(byte marker) {
    Constants result = null;
    for (Constants c : values()) {
      if (c.typeMarker == marker) {
        result = c;
      }
    }
    return result;
  }
  
  public static Constants fromClass(Class<?> clazz) {
    Constants result = null;
    for (Constants c : values()) {
      if (c.clazz != null && c.clazz.equals(clazz)) {
        result = c;
      }
    }
    if (result == null) {
      throw new IllegalArgumentException("Unknown class: " + clazz);
    }
    return result;
  }
}
