package io.github.simbo1905.no.framework;

import java.nio.ByteBuffer;

public interface SerializationSession {
  <T> void write(T testRecord);

  ByteBuffer flip();
}
