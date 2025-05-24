package io.github.simbo1905.no.framework;

import java.nio.ByteBuffer;

public interface ReadBuffer extends AutoCloseable {
  static ReadBuffer wrap(ByteBuffer buf) {
    return new ReadBufferImpl(buf);
  }

  static ReadBuffer wrap(byte[] bytes) {
    return new ReadBufferImpl(ByteBuffer.wrap(bytes));
  }

  boolean isClosed();

  int getVarInt();

  long getVarLong();

  boolean hasRemaining();

  int remaining();
}
