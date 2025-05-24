// SPDX-FileCopyrightText: 2025 Simon Massey
// SPDX-License-Identifier: MIT
package io.github.simbo1905.no.framework;

import java.nio.ByteBuffer;
import java.util.Arrays;

import static io.github.simbo1905.no.framework.Companion.maxSizeOf;

public interface WriteBuffer extends AutoCloseable {
  int position();

  ByteBuffer flip();

  boolean hasRemaining();

  int remaining();

  boolean isClosed();

  WriteBuffer putVarInt(int value);

  WriteBuffer putVarLong(long value);

  static WriteBuffer wrap(ByteBuffer buffer) {
    return new WriteBufferImpl(buffer);
  }

  static WriteBuffer of(int size) {
    return new WriteBufferImpl(ByteBuffer.allocate(size));
  }

  static WriteBuffer allocateSufficient(Object record) {
    return WriteBuffer.of(maxSizeOf(record));
  }

  static WriteBuffer allocateSufficient(Object[] records) {
    return WriteBuffer.of(Arrays.stream(records).mapToInt(Companion::maxSizeOf).sum());
  }
}
