// SPDX-FileCopyrightText: 2025 Simon Massey
// SPDX-License-Identifier: MIT

package io.github.simbo1905.no.framework;

import org.jetbrains.annotations.TestOnly;

import java.lang.reflect.Type;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.*;

import static io.github.simbo1905.no.framework.Constants.*;
import static io.github.simbo1905.no.framework.Pickler.LOGGER;

/// In order to get additional compression across many writes of nested records, enums or records with a sealed hierarchy
/// this class wraps a ByteBuffer and tracks the offsets of the record class names written into it. Rather than repeating
/// a class name it will write the offset of the first and only occurrence of that class name. This means that this
/// resource is not thread safe and may only be used once. The wrapped buffer must not be flipped. Call flip on this
/// class to flip the underlying buffer and mark this instance as closed.
class WriteBufferImpl implements WriteBuffer {

  final ByteBuffer buffer;
  final Map<InternedName, InternedPosition> offsetMap = new HashMap<>(64);
  final Map<String, Class<?>> nameToClass = new HashMap<>(64);
  final Map<Enum<?>, InternedName> enumToName = new HashMap<>(64);
  final List<Type[]> componentGenericTypes = new ArrayList<>();

  boolean closed = false;

  WriteBufferImpl(ByteBuffer buffer) {
    buffer.order(ByteOrder.BIG_ENDIAN);
    this.buffer = buffer;
  }

  static void write(ByteBuffer buffer, byte value) {
    LOGGER.finer(() -> "write(byte) - Enter: value=" + value + " position=" + buffer.position());
    buffer.put(BYTE.marker());
    buffer.put(value);
  }

  static void write(ByteBuffer buffer, char value) {
    LOGGER.finer(() -> "write(char) - Enter: value=" + value + " position=" + buffer.position());
    buffer.put(CHARACTER.marker());
    buffer.putChar(value);
  }

  static void write(ByteBuffer buffer, boolean value) {
    LOGGER.finer(() -> "write(boolean) - Enter: value=" + value + " position=" + buffer.position());
    buffer.put(BOOLEAN.marker());
    if (value) {
      buffer.put((byte) 1);
    } else {
      buffer.put((byte) 0);
    }
  }

  static void write(ByteBuffer buffer, String s) {
    LOGGER.finer(() -> "write(String) - Enter: value=" + s + " position=" + buffer.position());
    Objects.requireNonNull(s);
    buffer.put(STRING.marker());
    byte[] utf8 = s.getBytes(StandardCharsets.UTF_8);
    int length = utf8.length;
    ZigZagEncoding.putInt(buffer, length);
    buffer.put(utf8);
  }

  static void writeNull(ByteBuffer buffer) {
    buffer.put(NULL.marker());
  }

  static void write(ByteBuffer buffer, InternedName type) {
    Objects.requireNonNull(type);
    Objects.requireNonNull(type.name());
    buffer.put(INTERNED_NAME.marker());
    intern(buffer, type.name());
  }

  static void intern(ByteBuffer buffer, String string) {
    final var nameBytes = string.getBytes(StandardCharsets.UTF_8);
    final var nameLength = nameBytes.length;
    ZigZagEncoding.putInt(buffer, nameLength);
    buffer.put(nameBytes);
  }

  static void write(ByteBuffer buffer, InternedOffset typeOffset) {
    Objects.requireNonNull(typeOffset);
    final int offset = typeOffset.offset();
    final int size = ZigZagEncoding.sizeOf(offset);
    if (size < Integer.BYTES) {
      buffer.put(INTERNED_OFFSET_VAR.marker());
      ZigZagEncoding.putInt(buffer, offset);
    } else {
      buffer.put(INTERNED_OFFSET.marker());
      buffer.putInt(offset);
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

  public WriteBuffer putVarInt(int value) {
    validateNotClosed();
    ZigZagEncoding.putInt(buffer, value);
    return this;
  }

  public WriteBuffer putVarLong(long value) {
    validateNotClosed();
    ZigZagEncoding.putLong(buffer, value);
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
    return "WriteBufferImpl{" +
        "offsetMap=" + offsetMap +
        '}';
  }
}
