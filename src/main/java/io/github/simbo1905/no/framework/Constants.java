// SPDX-FileCopyrightText: 2025 Simon Massey
// SPDX-License-Identifier: MIT

package io.github.simbo1905.no.framework;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/// Enum containing constants used throughout the Pickler implementation
enum Constants {
  NULL((byte) 0, 0, null),
  BOOLEAN((byte) -1, 1, boolean.class),
  BYTE((byte) -2, Byte.BYTES, byte.class),
  SHORT((byte) -3, Short.BYTES, short.class),
  CHARACTER((byte) -4, Character.BYTES, char.class),
  INTEGER((byte) -5, Integer.BYTES, int.class),
  INTEGER_VAR((byte) -6, Integer.BYTES, int.class),
  LONG((byte) -7, Long.BYTES, long.class),
  LONG_VAR((byte) -8, Long.BYTES, long.class),
  FLOAT((byte) -9, Float.BYTES, float.class),
  DOUBLE((byte) -10, Double.BYTES, double.class),
  STRING((byte) -11, 0, String.class),
  OPTIONAL_EMPTY((byte) -12, 0, Optional.class),
  OPTIONAL_OF((byte) -13, 0, Optional.class),
  ENUM((byte) -14, 0, Enum.class),
  ARRAY((byte) -15, 0, null),
  MAP((byte) -16, 0, Map.class),
  LIST((byte) -17, 0, List.class),
  RECORD((byte) -18, 0, Record.class),
  SAME_TYPE((byte) -19, 0, Record.class),
  UUID((byte) -20, 16, java.util.UUID.class);

  final byte ordinal;
  final int sizeInBytes;
  final Class<?> clazz;

  Constants(byte ordinal, int sizeInBytes, Class<?> clazz) {
    this.ordinal = ordinal;
    this.sizeInBytes = sizeInBytes;
    this.clazz = clazz;
  }

  public static Constants fromMarker(byte marker) {
    Constants result = null;
    for (Constants c : values()) {
      if (c.ordinal == marker) {
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
