package io.github.simbo1905.no.framework;

import java.nio.ByteBuffer;

public interface ReadBuffer extends AutoCloseable {
  static ReadBuffer wrap(ByteBuffer buf) {
    return new ReadBufferImpl(buf);
  }

  boolean isClosed();

  int getInt();
}
