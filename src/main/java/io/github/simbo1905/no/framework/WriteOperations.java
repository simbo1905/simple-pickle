package io.github.simbo1905.no.framework;

import java.nio.ByteBuffer;
import java.util.Map;

public class WriteOperations {
  public static long readVarInt(ByteBuffer buf) {
    final var marker = buf.get();
    return (marker == WriteOperations.Constants.LONG_LEB128.typeMarker)
        ? decodeULEB128(buf)
        : decodeSLEB128(buf);
  }

  enum Constants {
    LONG_LEB128((byte) 0x01),
    LONG_SLEB128((byte) 0x02);

    final byte typeMarker;

    Constants(byte marker) {
      this.typeMarker = marker;
    }
  }

  static int writeVarInt(ByteBuffer buffer, long value) {
    if (value >= 0) {
      buffer.put(Constants.LONG_LEB128.typeMarker);
      return 1 + encodeULEB128(buffer, value);
    } else {
      buffer.put(Constants.LONG_SLEB128.typeMarker);
      return 1 + encodeSLEB128(buffer, value);
    }
  }

  static int encodeULEB128(ByteBuffer buffer, long value) {
    int written = 0;
    do {
      byte b = (byte) (value & 0x7F);
      value >>>= 7;
      if (value != 0) {
        b |= 0x80;
      }
      buffer.put(b);
      written++;
    } while (value != 0);
    return written;
  }

  static long decodeULEB128(ByteBuffer buffer) {
    long result = 0;
    int shift = 0;
    byte b;
    do {
      if (shift >= 64) {
        throw new RuntimeException("ULEB128 value exceeds maximum size");
      }
      b = buffer.get();
      result |= ((long) (b & 0x7F)) << shift;
      shift += 7;
    } while ((b & 0x80) != 0);
    return result;
  }

  static int encodeSLEB128(ByteBuffer buffer, long value) {
    int written = 0;
    boolean more;
    do {
      byte b = (byte) (value & 0x7F);
      // Check if sign bit of byte matches sign bit of value
      value >>= 7;
      more = !((value == 0 && (b & 0x40) == 0) ||
          (value == -1 && (b & 0x40) != 0));
      if (more) {
        b |= 0x80;
      }
      buffer.put(b);
      written++;
    } while (more);
    return written;
  }

  static long decodeSLEB128(ByteBuffer buffer) {
    long result = 0;
    int shift = 0;
    byte b;
    do {
      if (shift >= 64) {
        throw new RuntimeException("SLEB128 value exceeds maximum size");
      }
      b = buffer.get();
      result |= ((long) (b & 0x7F)) << shift;
      shift += 7;
    } while ((b & 0x80) != 0);

    // Sign extend if necessary
    if ((shift < 64) && ((b & 0x40) != 0)) {
      // Fill with 1s
      result |= (~0L) << shift;
    }
    return result;
  }


  static void write(Map<Class<?>, Integer> classToOffset, ByteBuffer buffer, Object c) {
        throw new AssertionError("not implemented");
    }

    static Object deserializeValue(Map<Integer, Class<?>> bufferOffset2Class, ByteBuffer buffer) {
        throw new AssertionError("Not implemented");
    }
}
