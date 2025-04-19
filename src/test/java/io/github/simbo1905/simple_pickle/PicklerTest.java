// SPDX-FileCopyrightText: 2025 Simon Massey
// SPDX-License-Identifier: Apache-2.0
package io.github.simbo1905.simple_pickle;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.logging.ConsoleHandler;
import java.util.logging.Level;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.assertEquals;

/// Test class for the Pickler functionality.
/// Demonstrates basic serialization and deserialization of records.
class PicklerTest {

  /// Set up logging before all tests
  @BeforeAll
  static void setupLogging() {
    // Configure the root logger to use FINE level
    Logger rootLogger = Logger.getLogger("");
    rootLogger.setLevel(Level.FINE);

    // Make sure the console handler also uses FINE level
    for (java.util.logging.Handler handler : rootLogger.getHandlers()) {
      if (handler instanceof ConsoleHandler) {
        handler.setLevel(Level.FINE);
      }
    }

    // Configure the PicklerGenerator logger specifically
    Logger picklerLogger = Logger.getLogger("io.github.simbo1905.simple_pickle.PicklerGenerator");
    picklerLogger.setLevel(Level.FINE);
  }

  /// A simple record for testing purposes
  record Person(String name, int age) {
  }
  
  @Test
  void testGenericPersonOneShot() {
    // Use the generic pickler
    final var original = new Person("Alice", 30);

    Pickler<Person> generated = Pickler.picklerForRecord(Person.class);

    // Serialize the Person record to a byte buffer
    final var buffer = ByteBuffer.allocate(1024);
    generated.serialize(original, buffer);
    buffer.flip(); // Prepare buffer for reading

    // Deserialize from the byte buffer
    final var deserialized = generated.deserialize(buffer);

    // Verify the deserialized object matches the original
    assertEquals(original, deserialized);
    assertEquals("Alice", deserialized.name());
    assertEquals(30, deserialized.age());
  }

  /// Tests manual implementation of Simple record serialization
  @Test
  void testGenericSimpleRecordStatics() {
    // Use the manually created pickler
    final var original = new Simple(42);

    Pickler<Simple> generated = Pickler.picklerForRecord(Simple.class);

    // Serialize the Simple record to a byte buffer
    final var buffer = ByteBuffer.allocate(1024);
    generated.serialize(original, buffer);
    buffer.flip(); // Prepare buffer for reading

    // Deserialize from the byte buffer
    final var deserialized = generated.deserialize(buffer);

    // Verify the deserialized object matches the original
    assertEquals(original, deserialized);
    assertEquals(42, deserialized.value());
  }

}
