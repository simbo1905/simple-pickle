package io.github.simbo1905.no.framework;

import org.jetbrains.annotations.TestOnly;

import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.IntStream;

/// In order to get additional compression across many writes of nested records, enums or records with a sealed hierarchy
/// this class wraps a ByteBuffer and tracks the offsets of the record class names written into it. Rather than repeating
/// a class name it will write the offset of the first and only occurrence of that class name. This means that this
/// resource is not thread safe and may only be used once. The wrapped buffer must not be flipped. Call flip on this
/// class to flip the underlying buffer and mark this instance as closed.
@SuppressWarnings("unused")
public class PackedBuffer implements AutoCloseable {

  final ByteBuffer buffer;
  final Map<InternedName, InternedPosition> offsetMap = new HashMap<>();
  boolean closed = false;

  PackedBuffer(ByteBuffer buffer) {
    buffer.order(ByteOrder.BIG_ENDIAN);
    this.buffer = buffer;
  }

  void validateNotClosed() {
    if (closed)
      throw new IllegalStateException("CompactedBuffer has been closed by flip() or close() and is now read-only");
  }

  public ByteBuffer put(byte b) {
    validateNotClosed();
    return buffer.put(b);
  }

  public ByteBuffer putChar(char value) {
    validateNotClosed();
    return buffer.putChar(value);
  }

  public ByteBuffer putShort(short value) {
    validateNotClosed();
    return buffer.putShort(value);
  }

  public ByteBuffer putInt(int value) {
    validateNotClosed();
    return buffer.putInt(value);
  }

  public ByteBuffer putLong(long value) {
    validateNotClosed();
    return buffer.putLong(value);
  }

  public ByteBuffer putFloat(int index, float value) {
    validateNotClosed();
    return buffer.putFloat(index, value);
  }

  public ByteBuffer putDouble(int index, double value) {
    validateNotClosed();
    return buffer.putDouble(index, value);
  }

  public ByteBuffer put(byte[] src) {
    validateNotClosed();
    return buffer.put(src);
  }

  int position() {
    validateNotClosed();
    return buffer.position();
  }

  /// Writes types into a buffer recursively. This is used to write out the components of a record.
  /// In order to prevent infinite loops the caller of the method must look up the pickler of any
  /// inner records and delegate to that pickler. This method will throw an exception if it is called
  /// to write out a record that is not the specific type of the pickler.
  /// @param object the class of the record
  /// @throws IllegalStateException if the buffer is closed
  int recursiveWrite(final PackedBuffer buf, Object object) {
    if (closed) throw new IllegalStateException("CompactedBuffer is closed");
    final var buffer = buf.buffer;
    return switch (object) {
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
      case InternedName t -> Companion.write(buffer, t);
      case InternedOffset t -> Companion.write(buffer, t);
      case Enum<?> e -> Companion.write(offsetMap, buffer, e);
      case Optional<?> o -> {
        int size = 1;
        if (o.isEmpty()) {
          buffer.put(Constants.OPTIONAL_EMPTY.marker());
        } else {
          buffer.put(Constants.OPTIONAL_OF.marker());
          size += recursiveWrite(buf, o.get());
        }
        yield size;
      }
      case List<?> l -> {
        buffer.put(Constants.LIST.marker());
        int size = 1 + ZigZagEncoding.putInt(buffer, l.size());
        for (Object item : l) {
          size += recursiveWrite(buf, item);
        }
        yield size;
      }
      case Map<?, ?> m -> {
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
