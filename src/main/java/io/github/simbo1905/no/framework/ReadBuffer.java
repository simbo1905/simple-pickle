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

  @Deprecated // TODO: Remove - these methods use reflection and violate no-reflection principle
  static ReadBuffer wrap(ByteBuffer buf) {
    return new ReadBufferImpl(buf, name -> {
      try {
        return Class.forName(name);
      } catch (ClassNotFoundException e) {
        throw new RuntimeException(e);
      }
    });
  }

  @Deprecated // TODO: Remove - these methods use reflection and violate no-reflection principle
  static ReadBuffer wrap(byte[] bytes) {
    return wrap(ByteBuffer.wrap(bytes));
  }

  boolean isClosed();

  int getVarInt();

  long getVarLong();

  boolean hasRemaining();

  int remaining();
}
