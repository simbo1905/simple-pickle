package io.github.simbo1905.no.framework;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import static io.github.simbo1905.no.framework.Constants.*;

record WriteOperations(Map<Class<?>, Integer> classToOffset, ByteBuffer buffer) {
  WriteOperations(ByteBuffer buffer) {
    this(new HashMap<>(), buffer);
  }

  int position() {
    return buffer.position();
  }

  public int write(int value) {
    if (ZigZagEncoding.sizeOf(value) < Integer.BYTES) {
      buffer.put(Constants.VAR_INT.marker());
      return 1 + ZigZagEncoding.putInt(buffer, value);
    } else {
      buffer.put(Constants.INTEGER.marker());
      buffer.putInt(value);
      return 1 + Integer.BYTES;
    }
  }

  public int write(long value) {
    if (ZigZagEncoding.sizeOf(value) < Long.BYTES) {
      buffer.put(VAR_LONG.marker());
      return 1 + ZigZagEncoding.putLong(buffer, value);
    } else {
      buffer.put(LONG.marker());
      buffer.putLong(value);
      return 1 + Long.BYTES;
    }
  }

  public Object read() {
    final byte marker = buffer.get();
    return switch (Constants.fromMarker(marker)) {
      case NULL -> null;
      case BOOLEAN -> buffer.get() != 0x0;
      case BYTE -> buffer.get();
      case SHORT -> buffer.getShort();
      case CHARACTER -> buffer.getChar();
      case INTEGER -> buffer.getInt();
      case VAR_INT -> ZigZagEncoding.getInt(buffer);
      case LONG -> buffer.getLong();
      case VAR_LONG -> ZigZagEncoding.getLong(buffer);
      case FLOAT -> buffer.getFloat();
      case DOUBLE -> buffer.getDouble();
      case STRING -> {
        int length = ZigZagEncoding.getInt(buffer);
        byte[] bytes = new byte[length];
        buffer.get(bytes);
        yield new String(bytes, StandardCharsets.UTF_8);
      }
      case OPTIONAL -> null;
      case RECORD -> null;
      case ARRAY -> null;
      case MAP -> null;
      case ENUM -> null;
      case LIST -> null;
    };
  }

  public int write(double value) {
    buffer.put(DOUBLE.marker());
    buffer.putDouble(value);
    return 1 + Double.BYTES;
  }

  public int write(float value) {
    buffer.put(FLOAT.marker());
    buffer.putFloat(value);
    return 1 + Float.BYTES;
  }

  public int write(short value) {
    buffer.put(SHORT.marker());
    buffer.putShort(value);
    return 1 + Short.BYTES;
  }

  public int write(char value) {
    buffer.put(CHARACTER.marker());
    buffer.putChar(value);
    return 1 + Character.BYTES;
  }

  public int write(boolean value) {
    buffer.put(BOOLEAN.marker());
    if (value) {
      buffer.put((byte) 1);
    } else {
      buffer.put((byte) 0);
    }
    return 1 + 1;
  }

  public int write(String s) {
    buffer.put(STRING.marker());
    byte[] utf8 = s.getBytes(StandardCharsets.UTF_8);
    int length = utf8.length;
    ZigZagEncoding.putInt(buffer, length); // TODO check max string size
    buffer.put(utf8);
    return 1 + length;
  }

  public <R extends Record> int write(R r) {
    throw new AssertionError("not implemented");
  }

  public int writeNull() {
    buffer.put(NULL.marker());
    return 1;
  }
}
