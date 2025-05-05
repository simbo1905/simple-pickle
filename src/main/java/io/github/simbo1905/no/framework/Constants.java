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
  LONG((byte) 7, Long.BYTES, long.class),
  FLOAT((byte) 8, Float.BYTES, float.class),
  DOUBLE((byte) 9, Double.BYTES, double.class),
  STRING((byte) 10, 0, String.class),
  OPTIONAL((byte) 11, 0, Optional.class),
  RECORD((byte) 12, 0, Record.class),
  ARRAY((byte) 13, 0, null),
  MAP((byte) 14, 0, Map.class),
  ENUM((byte) 15, 0, Enum.class),
  LIST((byte) 16, 0, List.class);

  private final byte typeMarker;
  private final int sizeInBytes;
  private final Class<?> clazz;

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
