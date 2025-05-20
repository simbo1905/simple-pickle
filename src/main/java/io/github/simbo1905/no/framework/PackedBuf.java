package io.github.simbo1905.no.framework;

import org.jetbrains.annotations.TestOnly;

import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.IntStream;

import static io.github.simbo1905.no.framework.Constants.*;
import static io.github.simbo1905.no.framework.Pickler.LOGGER;

/// In order to get additional compression across many writes of nested records, enums or records with a sealed hierarchy
/// this class wraps a ByteBuffer and tracks the offsets of the record class names written into it. Rather than repeating
/// a class name it will write the offset of the first and only occurrence of that class name. This means that this
/// resource is not thread safe and may only be used once. The wrapped buffer must not be flipped. Call flip on this
/// class to flip the underlying buffer and mark this instance as closed.
class PackedBuf implements PackedBuffer {

  final ByteBuffer buffer;
  final Map<InternedName, InternedPosition> offsetMap = new HashMap<>();
  boolean closed = false;

  PackedBuf(ByteBuffer buffer) {
    buffer.order(ByteOrder.BIG_ENDIAN);
    this.buffer = buffer;
  }

  static int write(ByteBuffer buffer, byte value) {
    LOGGER.finer(() -> "write(byte) - Enter: value=" + value + " position=" + buffer.position());
    buffer.put(BYTE.marker());
    buffer.put(value);
    return 1 + 1; // 1 byte for marker + 1 byte for value
  }

  static int write(ByteBuffer buffer, char value) {
    LOGGER.finer(() -> "write(char) - Enter: value=" + value + " position=" + buffer.position());
    buffer.put(CHARACTER.marker());
    buffer.putChar(value);
    return 1 + Character.BYTES;
  }

  static int write(ByteBuffer buffer, boolean value) {
    LOGGER.finer(() -> "write(boolean) - Enter: value=" + value + " position=" + buffer.position());
    buffer.put(BOOLEAN.marker());
    if (value) {
      buffer.put((byte) 1);
    } else {
      buffer.put((byte) 0);
    }
    return 1 + 1;
  }

  static int write(ByteBuffer buffer, String s) {
    LOGGER.finer(() -> "write(String) - Enter: value=" + s + " position=" + buffer.position());
    Objects.requireNonNull(s);
    buffer.put(STRING.marker());
    byte[] utf8 = s.getBytes(StandardCharsets.UTF_8);
    int length = utf8.length;
    ZigZagEncoding.putInt(buffer, length);
    buffer.put(utf8);
    return 1 + length;
  }

  static int writeNull(ByteBuffer buffer) {
    buffer.put(NULL.marker());
    return 1;
  }

  static int write(ByteBuffer buffer, InternedName type) {
    Objects.requireNonNull(type);
    Objects.requireNonNull(type.name());
    buffer.put(INTERNED_NAME.marker());
    return 1 + intern(buffer, type.name());
  }

  static int intern(ByteBuffer buffer, String string) {
    final var nameBytes = string.getBytes(StandardCharsets.UTF_8);
    final var nameLength = nameBytes.length;
    var size = ZigZagEncoding.putInt(buffer, nameLength);
    buffer.put(nameBytes);
    size += nameLength;
    return size;
  }

  static <T extends Enum<?>> int write(final Map<InternedName, InternedPosition> offsetMap, ByteBuffer buffer, T e) {
    Objects.requireNonNull(e);
    final var className = e.getDeclaringClass().getName();
    final var shortName = className.substring("".length()); // TODO shorten the enum class mame
    final var dotName = shortName + "." + e.name();
    final var internedName = new InternedName(dotName);
    if (!offsetMap.containsKey(internedName)) {
      offsetMap.put(internedName, new InternedPosition(buffer.position()));
      buffer.put(ENUM.marker());
      return 1 + intern(buffer, dotName);
    } else {
      final var internedPosition = offsetMap.get(internedName);
      final var internedOffset = new InternedOffset(internedPosition.position() - buffer.position());
      return write(buffer, internedOffset);
    }
  }

  static int write(ByteBuffer buffer, InternedOffset typeOffset) {
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

  void validateNotClosed() {
    if (closed)
      throw new IllegalStateException("CompactedBuffer has been closed by flip() or close() and is now read-only");
  }

  public void put(byte b) {
    validateNotClosed();
    buffer.put(b);
  }

  public PackedBuffer putInt(int value) {
    validateNotClosed();
    buffer.putInt(value);
    return this;
  }

  public int position() {
    validateNotClosed();
    return buffer.position();
  }

  /// Writes types into a buffer recursively. This is used to write out the components of a record.
  /// In order to prevent infinite loops the caller of the method must look up the pickler of any
  /// inner records and delegate to that pickler. This method will throw an exception if it is called
  /// to write out a record that is not the specific type of the pickler.
  /// @param object the class of the record
  /// @throws IllegalStateException if the buffer is closed
  int recursiveWrite(final PackedBuf buf, Object object) {
    if (closed) throw new IllegalStateException("CompactedBuffer is closed");
    final var buffer = buf.buffer;
    return switch (object) {
      case null -> writeNull(buffer);
      case Integer i -> Companion.write(buffer, i);
      case Long l -> Companion.write(buffer, l);
      case Short s -> Companion.write(buffer, s);
      case Byte b -> write(buffer, b);
      case Double d -> Companion.write(buffer, d);
      case Float f -> Companion.write(buffer, f);
      case Character ch -> write(buffer, ch);
      case Boolean b -> write(buffer, b);
      case String s -> write(buffer, s);
      case InternedName t -> write(buffer, t);
      case InternedOffset t -> write(buffer, t);
      case Enum<?> e -> write(offsetMap, buffer, e);
      case Optional<?> o -> {
        int size = 1;
        if (o.isEmpty()) {
          LOGGER.finer(() -> "write(empty) - position=" + buffer.position());
          buffer.put(Constants.OPTIONAL_EMPTY.marker());
        } else {
          LOGGER.finer(() -> "write(optional) - position=" + buffer.position());
          buffer.put(Constants.OPTIONAL_OF.marker());
          size += recursiveWrite(buf, o.get());
        }
        yield size;
      }
      case List<?> l -> {
        LOGGER.finer(() -> "write(list) - size=" + ZigZagEncoding.sizeOf(l.size()) + " position=" + buffer.position());
        buffer.put(Constants.LIST.marker());
        int size = 1 + ZigZagEncoding.putInt(buffer, l.size());
        for (Object item : l) {
          size += recursiveWrite(buf, item);
        }
        yield size;
      }
      case Map<?, ?> m -> {
        LOGGER.finer(() -> "write(map) - size=" + ZigZagEncoding.sizeOf(m.size()) + " position=" + buffer.position());
        buffer.put(Constants.MAP.marker());
        int size = 1 + ZigZagEncoding.putInt(buffer, m.size());
        for (Map.Entry<?, ?> entry : m.entrySet()) {
          size += recursiveWrite(buf, entry.getKey());
          size += recursiveWrite(buf, entry.getValue());
        }
        yield size;
      }
      // TODO zigzag compress long[] and int[]
      case Object a when a.getClass().isArray() -> {
        LOGGER.finer(() -> "write(array) - size=" + ZigZagEncoding.sizeOf(Array.getLength(a)) + " position=" + buffer.position());
        buffer.put(Constants.ARRAY.marker());
        final var InternedName = new InternedName(a.getClass().getComponentType().getName());
        int size = 1;
        size += recursiveWrite(buf, InternedName);
        int length = Array.getLength(a);
        size += ZigZagEncoding.putInt(buffer, length);
        if (byte.class.equals(a.getClass().getComponentType())) {
          buffer.put((byte[]) a);
          size += ((byte[]) a).length;
        } else {
          size += IntStream.range(0, length).map(i -> recursiveWrite(buf, Array.get(a, i))).sum();
        }
        yield size;
      }
      default -> throw new IllegalStateException("Unexpected value: " + object);
    };
  }

  /// Once a buffer has been closed all that can be done is flip to get the underlying buffer to read from it.
  @Override
  public void close() {
    this.closed = true;
  }

  /// Flips the buffer and closes this instance. The returned return buffer should be completely used not compacted.
  /// FIXME: should we track the start and end position of writes to the buffer and return a slice of it?
  public ByteBuffer flip() {
    buffer.flip();
    close();
    return buffer;
  }

  public boolean hasRemaining() {
    validateNotClosed();
    return buffer.hasRemaining();
  }

  public int remaining() {
    validateNotClosed();
    return buffer.remaining();
  }

  @TestOnly
  public void put(int i, byte maliciousByte) {
    validateNotClosed();
    buffer.put(i, maliciousByte);
  }
}
