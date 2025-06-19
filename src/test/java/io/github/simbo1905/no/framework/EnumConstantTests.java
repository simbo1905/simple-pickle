// SPDX-FileCopyrightText: 2025 Simon Massey
// SPDX-License-Identifier: Apache-2.0
//
package io.github.simbo1905.no.framework;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

/// Tests for enum constants in sealed interfaces - reproduces Paxos benchmark bug
class EnumConstantTests {

  /// Simple enum to test serialization
  enum Operation {
    NOOP, READ, WRITE, DELETE
  }

  /// Sealed interface with enum constant (reproduces bug pattern)
  sealed interface Command permits NoOperation, DataCommand {

    /// This constant causes serialization bug - NoOperation.NOOP fails with NPE
    NoOperation NOOP_INSTANCE = NoOperation.NOOP;
  }

  /// Record implementing sealed interface with enum constant
  record NoOperation(Operation op) implements Command {
    static final NoOperation NOOP = new NoOperation(Operation.NOOP);
  }

  /// Record with data implementing sealed interface
  record DataCommand(byte[] data, Operation op) implements Command {
  }

  @Test
  void testEnumConstantSerialization() {
    final var pickler = Pickler.forClass(Command.class);
    assertDoesNotThrow(() -> {
      final var buffer = ByteBuffer.allocate(1024);
      pickler.serialize(buffer, NoOperation.NOOP);
    }, "Enum constant serialization should not throw NPE");
  }

  @Test
  void testEnumInRecordSerialization() {
    final var pickler = Pickler.forClass(NoOperation.class);
    final var operation = new NoOperation(Operation.READ);

    assertDoesNotThrow(() -> {
      final var buffer = ByteBuffer.allocate(256);
      pickler.serialize(buffer, operation);
    }, "Enum in record serialization should work");
  }
}
