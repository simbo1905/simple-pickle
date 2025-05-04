// SPDX-FileCopyrightText: 2025 Simon Massey
// SPDX-License-Identifier: Apache-2.0
package io.github.simbo1905.no.framework;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.logging.ConsoleHandler;
import java.util.logging.Level;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.assertThrows;

/// Tests related to security aspects of serialization/deserialization.
class SecurityTest {

  private static final Logger LOGGER = Logger.getLogger(SecurityTest.class.getName());

  static {
    // Set up logging
    LOGGER.setLevel(Level.FINE);
    ConsoleHandler handler = new ConsoleHandler();
    handler.setLevel(Level.FINE);
    LOGGER.addHandler(handler);
  }

  sealed interface MyInterface permits Good {
  }

  /// Record for deserialization attack test
  record Good(String value) implements MyInterface {
  }

  /// Non-record class with the same name length as Good
  @SuppressWarnings("unused")
  static class Bad1 {
    String value;
  }

  /// Non-record class with the same name length as Good
  @SuppressWarnings("unused")
  static class Bad2 {
    String value;
    // Constructor or methods if needed, but not required for the attack test structure
    // Ensure it's not a record
  }

  @Test
  void testSealedTraitNotRecordAttack() {
    // 1. Get Pickler for the sealed trait
    final Pickler<MyInterface> pickler = Pickler.forSealedInterface(MyInterface.class);

    // 2. Create an instance of a permitted subtype
    final var original = new Good("safe_value");

    // 3. Serialize the instance
    final int size = pickler.sizeOf(original);
    final ByteBuffer buffer = ByteBuffer.allocate(size);
    pickler.serialize(original, buffer);
    buffer.flip(); // Prepare for reading/manipulation

    // 4. Manipulate the byte buffer to replace the class name
    // The format for sealed trait is: [classNameLength (int)] [classNameBytes (utf8)] [actual object data...]
    final int classNamePosition = buffer.position() + 4; // Position where class name bytes start

    final String maliciousClassName = "Bad1";

    final byte[] maliciousBytes = maliciousClassName.getBytes(StandardCharsets.UTF_8);

    // Overwrite the class name bytes in the buffer
    for (int i = 0; i < maliciousBytes.length; i++) {
      buffer.put(classNamePosition + i, maliciousBytes[i]);
    }

    // 5. Reset buffer position and attempt deserialization
    buffer.position(0); // Reset position to the beginning for deserialization

    // 6. Assert that deserialization fails because "Baad" is not a permitted subtype
    assertThrows(IllegalArgumentException.class, () -> {
      pickler.deserialize(buffer);
    }, "Deserialization should fail for non-record class");
  }

  @Test
  void testSealedTraitWrongRecordAttack() {
    // 1. Get Pickler for the sealed trait
    final Pickler<MyInterface> pickler = Pickler.forSealedInterface(MyInterface.class);

    // 2. Create an instance of a permitted subtype
    final var original = new Good("safe_value");

    // 3. Serialize the instance
    final int size = pickler.sizeOf(original);
    final ByteBuffer buffer = ByteBuffer.allocate(size);
    pickler.serialize(original, buffer);
    buffer.flip(); // Prepare for reading/manipulation

    // 4. Manipulate the byte buffer to replace the class name
    // The format for sealed trait is: [classNameLength (int)] [classNameBytes (utf8)] [actual object data...]
    final int classNamePosition = buffer.position() + 4; // Position where class name bytes start

    final String maliciousClassName = "Bad2";

    final byte[] maliciousBytes = maliciousClassName.getBytes(StandardCharsets.UTF_8);

    // Overwrite the class name bytes in the buffer
    for (int i = 0; i < maliciousBytes.length; i++) {
      buffer.put(classNamePosition + i, maliciousBytes[i]);
    }

    // 5. Reset buffer position and attempt deserialization
    buffer.position(0); // Reset position to the beginning for deserialization

    // 6. Assert that deserialization fails because "Bad2" is not a permitted subtype
    assertThrows(IllegalArgumentException.class, () -> {
      pickler.deserialize(buffer);
    }, "Deserialization should fail for wrong record type");
  }
}
