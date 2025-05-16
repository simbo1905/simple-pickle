package io.github.simbo1905.no.framework;

import org.jetbrains.annotations.TestOnly;

import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.IntStream;

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
  final Map<InternedName, InternedPosition> offsetMap = new HashMap<>();
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
  int writeComponent(final PackedBuffer buf, Object object) {
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
      case Enum<?> e -> Companion.write(offsetMap, buffer, "", e);
      case Optional<?> o -> {
        int size = 1;
        if (o.isEmpty()) {
          buffer.put(Constants.OPTIONAL_EMPTY.typeMarker);
        } else {
          buffer.put(Constants.OPTIONAL_OF.typeMarker);
          size += writeComponent(buf, o.get());
        }
        yield size;
      }
      case List<?> l -> {
        buffer.put(Constants.LIST.typeMarker);
        int size = 1 + ZigZagEncoding.putInt(buffer, l.size());
        for (Object item : l) {
          size += writeComponent(buf, item);
        }
        yield size;
      }
      case Map<?, ?> m -> {
        buffer.put(Constants.MAP.typeMarker);
        int size = 1 + ZigZagEncoding.putInt(buffer, m.size());
        for (Map.Entry<?, ?> entry : m.entrySet()) {
          size += writeComponent(buf, entry.getKey());
          size += writeComponent(buf, entry.getValue());
        }
        yield size;
      }
      // TODO zigzag compress long[] and int[]
      case Object a when a.getClass().isArray() -> {
        buffer.put(Constants.ARRAY.typeMarker);
        final var InternedName = new InternedName(a.getClass().getComponentType().getName());
        int size = 1;
        size += writeComponent(buf, InternedName);
        int length = Array.getLength(a);
        size += ZigZagEncoding.putInt(buffer, length);
        if (byte.class.equals(a.getClass().getComponentType())) {
          buffer.put((byte[]) a);
          size += ((byte[]) a).length;
        } else {
          size += IntStream.range(0, length).map(i -> writeComponent(buf, Array.get(a, i))).sum();
        }
        yield size;
      }
      default -> throw new AssertionError("unknown component type " + object.getClass());
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
    if (closed) throw new IllegalStateException("CompactedBuffer is closed");
    return buffer.hasRemaining();
  }

  public int remaining() {
    if (closed) throw new IllegalStateException("CompactedBuffer is closed");
    return buffer.remaining();
  }

  @TestOnly
  public void put(int i, byte maliciousByte) {
    if (closed) throw new IllegalStateException("CompactedBuffer is closed");
    buffer.put(i, maliciousByte);
  }
}
