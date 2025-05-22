package io.github.simbo1905.no.framework;

import java.nio.ByteBuffer;
import java.util.Arrays;

import static io.github.simbo1905.no.framework.Companion.maxSizeOf;

// SPDX-FileCopyrightText: 2025 Simon Massey
// SPDX-License-Identifier: MIT
public interface PackedBuffer extends AutoCloseable {
  int position();

  ByteBuffer flip();

  boolean hasRemaining();

  int remaining();

  PackedBuffer putInt(int size);

  void put(byte marker);

  boolean isClosed();

  static PackedBuffer wrap(ByteBuffer buffer) {
    return new PackedBufferImpl(buffer);
  }

  static PackedBuffer of(int size) {
    return new PackedBufferImpl(ByteBuffer.allocate(size));
  }

  static PackedBuffer allocateSufficient(Object record) {
    return PackedBuffer.of(maxSizeOf(record));
  }

  static PackedBuffer allocateSufficient(Object[] records) {
    return PackedBuffer.of(Arrays.stream(records).mapToInt(Companion::maxSizeOf).sum());
  }
}
