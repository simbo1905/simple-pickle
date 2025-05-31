// SPDX-FileCopyrightText: 2025 Simon Massey
// SPDX-License-Identifier: MIT
package io.github.simbo1905.no.framework;

import java.nio.ByteBuffer;
import java.util.function.Function;

@SuppressWarnings("UnusedReturnValue") // TODO use in tests
public interface WriteBuffer extends AutoCloseable {
  int position();

  ByteBuffer flip();

  boolean hasRemaining();

  int remaining();

  boolean isClosed();

  WriteBuffer putVarInt(int value);

  WriteBuffer putVarLong(long value);

  static WriteBuffer wrap(ByteBuffer buffer, Function<Class<?>, String> classToInternedName) {
    return new WriteBufferImpl(buffer, classToInternedName);
  }
}
