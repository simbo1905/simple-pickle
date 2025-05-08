package io.github.simbo1905.no.framework;

import java.nio.ByteBuffer;
import java.util.Optional;

import static io.github.simbo1905.no.framework.Pickler.LOGGER;
import static java.util.Optional.ofNullable;

/// This is a way to pass down either a dry run of writing things or the actual work
/// During the dry run we just count the size of the work we would have written so that we can add up the zigzag varit ssizes
record Work(int[] box, Optional<ByteBuffer> buffer) {
  static Work of(ByteBuffer buffer) {
    return new Work(new int[]{0}, ofNullable(buffer));
  }

  static Work of() {
    return new Work(new int[]{0}, Optional.empty());
  }

  public int size() {
    return box[0];
  }

  public int add(int size) {
    box[0] += size;
    return size;
  }

  public void putShort(short value) {
    add(ZigZagEncoding.sizeOf(value));
    buffer.ifPresent(byteBuffer -> ZigZagEncoding.putInt(byteBuffer, value));
  }

  public short getShort() {
    return buffer.map(byteBuffer -> (short) ZigZagEncoding.getInt(byteBuffer)).orElse((short) 0);
  }

  public Work put(byte marker) {
    add(1);
    buffer.ifPresent(byteBuffer -> byteBuffer.put(marker));
    return this;
  }

  public void putInt(int value) {
    buffer.ifPresent(byteBuffer -> {
      LOGGER.finer("putInt position: " + byteBuffer.position());
    });
    final var size = buffer.map(byteBuffer -> ZigZagEncoding.putInt(byteBuffer, value)).orElse(ZigZagEncoding.sizeOf(value));
    add(size);
  }

  public int getInt() {
    buffer.ifPresent(byteBuffer -> {
      LOGGER.finer("getInt position: " + byteBuffer.position());
    });
    return buffer.map(ZigZagEncoding::getInt).orElse(0);
//    return buffer.map(ByteBuffer::getInt).orElse(0);
  }

  public void put(byte[] c) {
    add(c.length);
    buffer.ifPresent(byteBuffer -> byteBuffer.put(c));
  }

  public void putLong(Long l) {
    add(Long.BYTES);
    buffer.ifPresent(byteBuffer -> byteBuffer.putLong(l));
  }

  public void putDouble(Double d) {
    add(Double.BYTES);
    buffer.ifPresent(byteBuffer -> byteBuffer.putDouble(d));
  }

  public void putFloat(Float f) {
    add(Float.BYTES);
    buffer.ifPresent(byteBuffer -> byteBuffer.putFloat(f));
  }

  public void putChar(Character ch) {
    add(Character.BYTES);
    buffer.ifPresent(byteBuffer -> byteBuffer.putChar(ch));
  }

  public int position() {
    return buffer.map(ByteBuffer::position).orElse(0);
  }


  public byte get() {
    return buffer.map(ByteBuffer::get).orElse((byte) 0);
  }

  public Object getLong() {
    return buffer.map(ByteBuffer::getLong).orElse(0L);
  }

  public Object getDouble() {
    return buffer.map(ByteBuffer::getDouble).orElse(0.0);
  }

  public Object getFloat() {
    return buffer.map(ByteBuffer::getFloat).orElse(0.0f);
  }

  public Object getChar() {
    return buffer.map(ByteBuffer::getChar).orElse((char) 0);
  }

  public void get(byte[] bytes) {
    buffer.ifPresent(byteBuffer -> byteBuffer.get(bytes));
  }

  public int remaining() {
    return buffer.map(ByteBuffer::remaining).orElse(0);
  }
}
