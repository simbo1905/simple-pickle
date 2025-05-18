package io.github.simbo1905.no.framework;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.github.simbo1905.no.framework.Pickler.LOGGER;

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
  INTERNED_NAME((byte) 15, 0, InternedName.class),
  INTERNED_OFFSET((byte) 16, 0, InternedOffset.class),
  INTERNED_OFFSET_VAR((byte) 17, 0, InternedOffset.class),
  ENUM((byte) 18, 0, Enum.class),
  ARRAY((byte) 19, 0, null),
  MAP((byte) 20, 0, Map.class),
  LIST((byte) 21, 0, List.class),
  RECORD((byte) 22, 0, Record.class),
  SAME_TYPE((byte) 23, 0, Record.class);

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

  public int getSizeInBytes() {
    return sizeInBytes;
  }

  public Class<?> _class() {
    return clazz;
  }

  public static Constants fromMarker(byte marker) {
    for (Constants c : values()) {
      if (c.typeMarker == marker) {
        return c;
      }
    }
    final var msg = "Unknown type marker: " + marker;
    LOGGER.severe(() -> msg);
    throw new IllegalArgumentException(msg);
  }
}
