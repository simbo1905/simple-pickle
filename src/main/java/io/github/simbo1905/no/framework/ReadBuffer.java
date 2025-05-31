package io.github.simbo1905.no.framework;

import java.nio.ByteBuffer;
import java.util.function.Function;

public interface ReadBuffer extends AutoCloseable {
  static ReadBuffer wrap(ByteBuffer buf, Function<String, Class<?>> internedNameToClass) {
    return new ReadBufferImpl(buf, internedNameToClass);
  }

  static ReadBuffer wrap(byte[] bytes, Function<String, Class<?>> internedNameToClass) {
    return new ReadBufferImpl(ByteBuffer.wrap(bytes), internedNameToClass);
  }

  boolean isClosed();

  int getVarInt();

  long getVarLong();

  boolean hasRemaining();

  int remaining();
}
