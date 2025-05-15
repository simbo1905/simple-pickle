package io.github.simbo1905.no.framework;

import org.jetbrains.annotations.TestOnly;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static io.github.simbo1905.no.framework.Constants0.*;

/// This class tracks the written position of record class names so that they can be referenced by an offset.
/// It also uses ZigZag encoding to reduce the size of whole numbers data written to the buffer.
public class CompactedBuffer implements AutoCloseable {
  final ByteBuffer buffer;
  final Map<RecordPickler0.InternedName, RecordPickler0.InternedPosition> offsetMap = new HashMap<>();
  boolean closed = false;

  CompactedBuffer(ByteBuffer buffer) {
    this.buffer = buffer;
  }

  int position() {
    return buffer.position();
  }

  public int write(int value) {
    if (ZigZagEncoding.sizeOf(value) < Integer.BYTES) {
      buffer.put(Constants0.INTEGER_VAR.marker());
      return 1 + ZigZagEncoding.putInt(buffer, value);
    } else {
      buffer.put(Constants0.INTEGER.marker());
      buffer.putInt(value);
      return 1 + Integer.BYTES;
    }
  }

  public int write(long value) {
    if (ZigZagEncoding.sizeOf(value) < Long.BYTES) {
      buffer.put(LONG_VAR.marker());
      return 1 + ZigZagEncoding.putLong(buffer, value);
    } else {
      buffer.put(LONG.marker());
      buffer.putLong(value);
      return 1 + Long.BYTES;
    }
  }

  @TestOnly
  Object read() {
    final byte marker = buffer.get();
    return switch (Constants0.fromMarker(marker)) {
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
        yield new RecordPickler0.InternedName(new String(bytes, StandardCharsets.UTF_8));
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
        yield new RecordPickler0.InternedName(new String(bytes, StandardCharsets.UTF_8));
      }
      case INTERNED_OFFSET_VAR -> {
        final int highWaterMark = buffer.position();
        final int offset = ZigZagEncoding.getInt(buffer);
        buffer.position(buffer.position() + offset - 1);
        int length = ZigZagEncoding.getInt(buffer);
        byte[] bytes = new byte[length];
        buffer.get(bytes);
        buffer.position(highWaterMark);
        yield new RecordPickler0.InternedName(new String(bytes, StandardCharsets.UTF_8));
      }
      case ARRAY -> throw new AssertionError("not implemented 1");
      case MAP -> throw new AssertionError("not implemented 2");
      case LIST -> throw new AssertionError("not implemented 4");
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
    Objects.requireNonNull(s);
    buffer.put(STRING.marker());
    byte[] utf8 = s.getBytes(StandardCharsets.UTF_8);
    int length = utf8.length;
    ZigZagEncoding.putInt(buffer, length); // TODO check max string size
    buffer.put(utf8);
    return 1 + length;
  }

  public int writeNull() {
    buffer.put(NULL.marker());
    return 1;
  }

  public int write(RecordPickler0.InternedName type) {
    Objects.requireNonNull(type);
    Objects.requireNonNull(type.name());
    buffer.put(INTERNED_NAME.marker());
    return 1 + intern(type.name());
  }

  int intern(String string) {
    final var nameBytes = string.getBytes(StandardCharsets.UTF_8);
    final var nameLength = nameBytes.length;
    var size = ZigZagEncoding.putInt(buffer, nameLength);
    buffer.put(nameBytes);
    size += nameLength;
    return size;
  }

  public <T extends Enum<?>> int write(String ignoredPrefix, T e) {
    Objects.requireNonNull(e);
    Objects.requireNonNull(ignoredPrefix);
    final var className = e.getDeclaringClass().getName();
    final var shortName = className.substring(ignoredPrefix.length());
    final var dotName = shortName + "." + e.name();
    final var internedName = new RecordPickler0.InternedName(dotName);
    if (!offsetMap.containsKey(internedName)) {
      offsetMap.put(internedName, new RecordPickler0.InternedPosition(buffer.position()));
      buffer.put(ENUM.marker());
      return 1 + intern(dotName);
    } else {
      final var internedPosition = offsetMap.get(internedName);
      final var internedOffset = new RecordPickler0.InternedOffset(internedPosition.position() - buffer.position());
      return write(internedOffset);
    }
  }

  public int write(RecordPickler0.InternedOffset typeOffset) {
    Objects.requireNonNull(typeOffset);
    final int offset = typeOffset.offset();
    final int size = ZigZagEncoding.sizeOf(offset);
    if (size < Integer.BYTES) {
      buffer.put(INTERNED_OFFSET_VAR.marker());
      return 1 + ZigZagEncoding.putInt(buffer, offset);
    } else {
      buffer.put(INTERNED_OFFSET.marker());
      buffer.putInt(offset);
      return 1 + Integer.BYTES;
    }
  }

  public <T> int write(@SuppressWarnings("OptionalUsedAsFieldOrParameterType") Optional<T> optional) {
    if (optional.isPresent()) {
      buffer.put(OPTIONAL_OF.marker());
      final T value = optional.get();
      final int innerSize = switch (value) {
        case Integer i -> write(i);
        case Long l -> write(l);
        case Short s -> write(s);
        case Byte b -> write(b);
        case Double d -> write(d);
        case Float f -> write(f);
        case Character c -> write(c);
        case Boolean b -> write(b);
        case String s -> write(s);
        case Optional<?> o -> write(o);
        case RecordPickler0.InternedName t -> write(t);
        case RecordPickler0.InternedOffset t -> write(t);
        default -> throw new AssertionError("unknown optional value " + value);
      };
      return 1 + innerSize;
    } else {
      buffer.put(OPTIONAL_EMPTY.marker());
      return 1;
    }
  }

  /// writes a record class name to the buffer
  /// @param object the class of the record
  /// @throws IllegalStateException if the buffer is closed
  public void writeComponent(Object object) {
    if (closed) throw new IllegalStateException("CompactedBuffer is closed");
    switch (object) {
      case null -> writeNull();
      case Integer i -> write(i);
      case Long l -> write(l);
      case Short s -> write(s);
      case Byte b -> write(b);
      case Double d -> write(d);
      case Float f -> write(f);
      case Character ch -> write(ch);
      case Boolean b -> write(b);
      case String s -> write(s);
      case RecordPickler0.InternedName t -> write(t);
      case RecordPickler0.InternedOffset t -> write(t);
      case Enum<?> e -> write("", e);
      default -> throw new AssertionError("unknown component type " + object.getClass());
    }
  }

  @Override
  public void close() {
    this.closed = true;
  }

  @TestOnly
  void flip() {
    buffer.flip();
  }
}
