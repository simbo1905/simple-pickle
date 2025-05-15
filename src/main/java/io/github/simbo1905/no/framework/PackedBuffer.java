package io.github.simbo1905.no.framework;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.github.simbo1905.no.framework.Pickler0.LOGGER;

/// This class tracks the written position of record class names so that they can be referenced by an offset.
/// It also uses ZigZag encoding to reduce the size of whole numbers data written to the buffer.
@SuppressWarnings("unused")
public class PackedBuffer implements AutoCloseable {
  public ByteBuffer put(byte b) {
    if (closed)
      throw new IllegalStateException("CompactedBuffer has been closed by flip() or close() and is now read-only");
    return buffer.put(b);
  }

  public ByteBuffer putChar(char value) {
    return buffer.putChar(value);
  }

  public ByteBuffer putShort(short value) {
    return buffer.putShort(value);
  }

  public ByteBuffer putInt(int value) {
    return buffer.putInt(value);
  }

  public ByteBuffer putLong(long value) {
    return buffer.putLong(value);
  }

  public ByteBuffer putFloat(int index, float value) {
    return buffer.putFloat(index, value);
  }

  public ByteBuffer putDouble(int index, double value) {
    return buffer.putDouble(index, value);
  }

  public ByteBuffer put(byte[] src) {
    return buffer.put(src);
  }

  final ByteBuffer buffer;
  final Map<Pickler.InternedName, Pickler.InternedPosition> offsetMap = new HashMap<>();
  boolean closed = false;

  PackedBuffer(ByteBuffer buffer) {
    this.buffer = buffer;
  }

  int position() {
    return buffer.position();
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

  /// Once a buffer has been closed all that can be done is flip to get the underlying buffer to read from it.
  @Override
  public void close() {
    this.closed = true;
  }

  /// Flips the buffer and closes this instance. The returned return buffer should be completely used not compacted.
  /// FIXME: should we track the start and end possition of writes to the buffer and return a slice of it?
  public ByteBuffer flip() {
    buffer.flip();
    close();
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
