package io.github.simbo1905.no.framework;

import java.nio.ByteBuffer;

/// SPDX-FileCopyrightText: 2025 Simon Massey
/// SPDX-License-Identifier: MIT
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
}
