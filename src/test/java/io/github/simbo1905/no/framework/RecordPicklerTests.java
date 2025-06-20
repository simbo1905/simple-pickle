// SPDX-FileCopyrightText: 2025 Simon Massey
// SPDX-License-Identifier: Apache-2.0
//
package io.github.simbo1905.no.framework;

import org.junit.jupiter.api.*;

import java.nio.ByteBuffer;

import static io.github.simbo1905.no.framework.Pickler.LOGGER;
import static org.assertj.core.api.Assertions.assertThat;

public class RecordPicklerTests {

  // inner record
  record Boxed(int value) {
  }

  // inner interface of enum and record
  public sealed interface Link permits LinkedRecord, LinkEnd {
  }

  // record implementing interface
  public record LinkedRecord(Boxed value, Link next) implements Link {
  }

  // enum implementing interface
  public record LinkEnd() implements Link {
  }

  // A linked list with two nodes to test with
  static Link linkedListTwoNodes =
      new LinkedRecord(new Boxed(1),
          new LinkedRecord(new Boxed(2),
              new LinkEnd()));

  @BeforeAll
  static void setupLogging() {
    io.github.simbo1905.LoggingControl.setupCleanLogging();
  }

  @BeforeEach
  void setUp() {
    LOGGER.fine(() -> "Starting RecordPicklerTests test");
  }

  @AfterEach
  void tearDown() {
    LOGGER.fine(() -> "Finished RecordPicklerTests test");
  }

  @Test
  @DisplayName("Test record value round trips")
  void testBoxedRoundTrip() {
    // Create a pickler for the Boxed record
    Pickler<Boxed> pickler = Pickler.forClass(Boxed.class);

    // Create an instance of Boxed
    Boxed original = new Boxed(42);

    // Serialize the original record to a ByteBuffer
    ByteBuffer buffer = ByteBuffer.allocate(1024);
    int size = pickler.serialize(buffer, original);
    buffer.flip(); // Prepare for reading

    // Deserialize the record from the ByteBuffer
    Boxed deserialized = pickler.deserialize(buffer);

    // Assert that the original and deserialized records are equal
    assertThat(deserialized).isEqualTo(original);

    // check the size is less than the maxSizeOf
    int maxSizeOf = pickler.maxSizeOf(original);
    assertThat(size).isLessThanOrEqualTo(maxSizeOf);
  }

  @Test
  @DisplayName("Test linked record serialization and deserialization")
  void testLinkedRecordRoundTrip() {
    // Create a pickler for the Link interface
    Pickler<Link> pickler = Pickler.forClass(Link.class);

    // Serialize the linked list with two nodes
    ByteBuffer buffer = ByteBuffer.allocate(1024);
    int size = pickler.serialize(buffer, linkedListTwoNodes);
    buffer.flip(); // Prepare for reading

    // Deserialize the linked list
    Link deserialized = pickler.deserialize(buffer);

    // Assert that the deserialized structure matches the original
    assertThat(deserialized).isEqualTo(linkedListTwoNodes);

    // check the size is less than the maxSizeOf
    int maxSizeOf = pickler.maxSizeOf(linkedListTwoNodes);
    assertThat(size).isLessThanOrEqualTo(maxSizeOf);
  }
}
