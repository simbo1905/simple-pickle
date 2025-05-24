package io.github.simbo1905.no.framework;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.HashMap;
import java.util.Map;

class ReadBufferImpl implements ReadBuffer {
  final ByteBuffer buffer;
  boolean closed = false;

  final Map<String, Enum<?>> nameToEnum = new HashMap<>(64);

  ReadBufferImpl(ByteBuffer buffer) {
    buffer.order(ByteOrder.BIG_ENDIAN);
    this.buffer = buffer;
  }

  @Override
  public void close() throws Exception {
    this.closed = true;
  }

  @Override
  public boolean isClosed() {
    return this.closed;
  }

  @Override
  public int getVarInt() {
    if (closed) {
      throw new IllegalStateException("Buffer is closed");
    }
    return ZigZagEncoding.getInt(buffer);
  }

  @Override
  public long getVarLong() {
    if (closed) {
      throw new IllegalStateException("Buffer is closed");
    }
    return ZigZagEncoding.getLong(buffer);
  }

  @Override
  public boolean hasRemaining() {
    return buffer.hasRemaining();
  }

  @Override
  public int remaining() {
    return buffer.remaining();
  }
}
