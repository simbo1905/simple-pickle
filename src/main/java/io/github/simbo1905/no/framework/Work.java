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

  public void add(int size) {
    box[0] += size;
  }

  public Work putShort(short length) {
    add(Short.BYTES);
    buffer.ifPresent(byteBuffer -> byteBuffer.putShort(length));
    return this;
  }

  public Work put(byte marker) {
    add(1);
    buffer.ifPresent(byteBuffer -> byteBuffer.put(marker));
    return this;
  }

  public Work putInt(int length) {
    add(Integer.BYTES);
    buffer.ifPresent(byteBuffer -> byteBuffer.putInt(length));
    return this;
  }

  public Work put(byte[] c) {
    add(c.length);
    buffer.ifPresent(byteBuffer -> byteBuffer.put(c));
    return this;
  }

  public Work putLong(Long l) {
    add(Long.BYTES);
    buffer.ifPresent(byteBuffer -> byteBuffer.putLong(l));
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
}
