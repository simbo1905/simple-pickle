package io.github.simbo1905.no.framework;

import java.nio.ByteBuffer;
import java.util.Optional;

import static java.util.Optional.ofNullable;

/// This is a way to pass down either a dry run of writing things or the actual work
/// During the dry run we just count the size of the work we would have written so that we can add up the zigzag varit ssizes
record Work(int[] box, Optional<ByteBuffer> buffer) {
  static Work of(ByteBuffer buffer) {
    return new Work(new int[]{0}, ofNullable(buffer));
  }

  public int size() {
    return box[0];
  }

  public int add(int size) {
    box[0] += size;
    return size;
  }

  public int putShort(short length) {
    return add(buffer.map(byteBuffer -> ZigZagEncoding.putInt(byteBuffer, length)).orElse(ZigZagEncoding.sizeOf(length)));
  }

  public Work put(byte marker) {
    add(1);
    buffer.ifPresent(byteBuffer -> byteBuffer.put(marker));
    return this;
  }

  public Work putInt(int length) {
    int count = buffer.map(byteBuffer -> ZigZagEncoding.putInt(byteBuffer, length)).orElse(0);
    if (count > 0) {
      add(count);
    } else {
      add(ZigZagEncoding.sizeOf(length));
    }
    return this;
  }

  public Work put(byte[] c) {
    add(c.length);
    buffer.ifPresent(byteBuffer -> byteBuffer.put(c));
    return this;
  }

  public Work putLong(Long l) {
    int count = buffer.map(byteBuffer -> ZigZagEncoding.putLong(byteBuffer, l)).orElse(0);
    if (count > 0) {
      add(count);
    } else {
      add(ZigZagEncoding.sizeOf(l));
    }
    return this;
  }

  public Work putDouble(Double d) {
    add(Double.BYTES);
    buffer.ifPresent(byteBuffer -> byteBuffer.putDouble(d));
    return this;
  }

  public Work putFloat(Float f) {
    add(Float.BYTES);
    buffer.ifPresent(byteBuffer -> byteBuffer.putFloat(f));
    return this;
  }

  public Work putChar(Character ch) {
    add(Character.BYTES);
    buffer.ifPresent(byteBuffer -> byteBuffer.putChar(ch));
    return this;
  }

  public int position() {
    return buffer.map(ByteBuffer::position).orElse(0);
  }

  public short getShort() {
    return buffer.map(ZigZagEncoding::getInt).orElse(0).shortValue();
  }

  public byte get() {
    return buffer.map(ByteBuffer::get).orElse((byte) 0);
  }

  public int getInt() {
    return buffer.map(ZigZagEncoding::getInt).orElse(0);
  }

  public Object getLong() {
    return buffer.map(ZigZagEncoding::getLong).orElse(0L);
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
