// SPDX-FileCopyrightText: 2025 Simon Massey
// SPDX-License-Identifier: MIT

package io.github.simbo1905.no.framework;

import org.jetbrains.annotations.TestOnly;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static io.github.simbo1905.no.framework.Constants.*;
import static io.github.simbo1905.no.framework.Pickler.LOGGER;

/// In order to get additional compression across many writes of nested records, enums or records with a sealed hierarchy
/// this class wraps a ByteBuffer and tracks the offsets of the record class names written into it. Rather than repeating
/// a class name it will write the offset of the first and only occurrence of that class name. This means that this
/// resource is not thread safe and may only be used once. The wrapped buffer must not be flipped. Call flip on this
/// class to flip the underlying buffer and mark this instance as closed.
class WriteBufferImpl implements WriteBuffer {

  final ByteBuffer buffer;
  final Map<InternedName, InternedPosition> offsetMap = new HashMap<>();
  final Map<String, Class<?>> nameToClass = new HashMap<>();
  final Map<Enum<?>, InternedName> enumToName = new HashMap<>();

  boolean closed = false;

  WriteBufferImpl(ByteBuffer buffer) {
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

  public WriteBuffer putInt(int value) {
    validateNotClosed();
    buffer.putInt(value);
    return this;
  }

  public int position() {
    validateNotClosed();
    return buffer.position();
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

  @Override
  public boolean isClosed() {
    return closed;
  }

  @TestOnly
  public void put(int i, byte maliciousByte) {
    validateNotClosed();
    buffer.put(i, maliciousByte);
  }

  @Override
  public String toString() {
    return "PackedBufferImpl{" +
        "buffer=" + buffer +
        ", offsetMap.keys()=" + offsetMap.keySet() +
        '}';
  }
}
