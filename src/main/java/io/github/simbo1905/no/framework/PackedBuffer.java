package io.github.simbo1905.no.framework;

import java.nio.ByteBuffer;

public interface PackedBuffer extends AutoCloseable {
  int position();

  ByteBuffer flip();

  boolean hasRemaining();

  int remaining();

  PackedBuffer putInt(int size);

  void put(byte marker);
}
