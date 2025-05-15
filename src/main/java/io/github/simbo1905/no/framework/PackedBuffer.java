package io.github.simbo1905.no.framework;

import org.jetbrains.annotations.TestOnly;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.github.simbo1905.no.framework.Pickler0.LOGGER;

/// This class tracks the written position of record class names so that they can be referenced by an offset.
/// It also uses ZigZag encoding to reduce the size of whole numbers data written to the buffer.
public class PackedBuffer implements AutoCloseable {
  final ByteBuffer buffer;
  final Map<Pickler.InternedName, Pickler.InternedPosition> offsetMap = new HashMap<>();
  boolean closed = false;

  PackedBuffer(ByteBuffer buffer) {
    this.buffer = buffer;
  }

  int position() {
    return buffer.position();
  }

  @TestOnly
  Object read() {
    final byte marker = buffer.get();
    return switch (Constants.fromMarker(marker)) {
      case NULL -> null;
      case BOOLEAN -> buffer.get() != 0x0;
      case BYTE -> buffer.get();
      case SHORT -> buffer.getShort();
      case CHARACTER -> buffer.getChar();
      case INTEGER -> buffer.getInt();
      case INTEGER_VAR -> ZigZagEncoding.getInt(buffer);
      case LONG -> buffer.getLong();
      case LONG_VAR -> ZigZagEncoding.getLong(buffer);
      case FLOAT -> buffer.getFloat();
      case DOUBLE -> buffer.getDouble();
      case STRING -> {
        int length = ZigZagEncoding.getInt(buffer);
        byte[] bytes = new byte[length];
        buffer.get(bytes);
        yield new String(bytes, StandardCharsets.UTF_8);
      }
      case OPTIONAL_EMPTY -> Optional.empty();
      case OPTIONAL_OF -> Optional.ofNullable(read());
      case INTERNED_NAME, ENUM -> {
        int length = ZigZagEncoding.getInt(buffer);
        byte[] bytes = new byte[length];
        buffer.get(bytes);
        yield new Pickler.InternedName(new String(bytes, StandardCharsets.UTF_8));
      }
      case INTERNED_OFFSET -> {
        final int offset = buffer.getInt();
        final int highWaterMark = buffer.position();
        final int newPosition = buffer.position() + offset - 2;
        buffer.position(newPosition);
        int length = ZigZagEncoding.getInt(buffer);
        byte[] bytes = new byte[length];
        buffer.get(bytes);
        buffer.position(highWaterMark);
        yield new Pickler.InternedName(new String(bytes, StandardCharsets.UTF_8));
      }
      case INTERNED_OFFSET_VAR -> {
        final int highWaterMark = buffer.position();
        final int offset = ZigZagEncoding.getInt(buffer);
        buffer.position(buffer.position() + offset - 1);
        int length = ZigZagEncoding.getInt(buffer);
        byte[] bytes = new byte[length];
        buffer.get(bytes);
        buffer.position(highWaterMark);
        yield new Pickler.InternedName(new String(bytes, StandardCharsets.UTF_8));
      }
      case ARRAY -> throw new AssertionError("not implemented 1");
      case MAP -> throw new AssertionError("not implemented 2");
      case LIST -> throw new AssertionError("not implemented 4");
    };
  }

  /// writes a record class name to the buffer
  /// @param object the class of the record
  /// @throws IllegalStateException if the buffer is closed
  void writeComponent(final PackedBuffer buf, Object object) {
    if (closed) throw new IllegalStateException("CompactedBuffer is closed");
    final var buffer = buf.buffer;
    switch (object) {
      case null -> Companion.writeNull(buffer);
      case Integer i -> Companion.write(buffer, i);
      case Long l -> Companion.write(buffer, l);
      case Short s -> Companion.write(buffer, s);
      case Byte b -> Companion.write(buffer, b);
      case Double d -> Companion.write(buffer, d);
      case Float f -> Companion.write(buffer, f);
      case Character ch -> Companion.write(buffer, ch);
      case Boolean b -> Companion.write(buffer, b);
      case String s -> Companion.write(buffer, s);
      case Pickler.InternedName t -> Companion.write(buffer, t);
      case Pickler.InternedOffset t -> Companion.write(buffer, t);
      case Enum<?> e -> Companion.write(offsetMap, buffer, "", e);
      default -> throw new AssertionError("unknown component type " + object.getClass());
    }
  }

  @Override
  public void close() {
    this.closed = true;
  }

  @TestOnly
  public ByteBuffer flip() {
    buffer.flip();
    return buffer;
  }

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
    INTERNED_NAME((byte) 15, 0, Pickler.InternedName.class),
    INTERNED_OFFSET((byte) 16, 0, Pickler.InternedOffset.class),
    INTERNED_OFFSET_VAR((byte) 17, 0, Pickler.InternedOffset.class),
    ENUM((byte) 18, 0, Enum.class),
    ARRAY((byte) 19, 0, null),
    MAP((byte) 20, 0, Map.class),
    LIST((byte) 21, 0, List.class);

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
}
