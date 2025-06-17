package io.github.simbo1905.no.framework;

import org.junit.jupiter.api.*;

import static io.github.simbo1905.no.framework.Pickler.LOGGER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

/// Package-private tests for core machinery components
/// Tests the internal implementation details that are not part of the public API
class MetaProgrammingTests {

  @BeforeAll
  static void setupLogging() {
    io.github.simbo1905.LoggingControl.setupCleanLogging();
  }

  @BeforeEach
  void setUp() {
    LOGGER.fine(() -> "Starting MetaProgrammingTests test");
  }

  @AfterEach
  void tearDown() {
    LOGGER.fine(() -> "Finished MetaProgrammingTests test");
  }

  /// Nested record to test discovery
  public record PrimitiveValueRecord(boolean bool, byte b, char c, short s, int i, long l, float f, double d) {}

  static PrimitiveValueRecord primitiveValueRecord =
    new PrimitiveValueRecord(true, (byte) 1, 'a', (short) 2, 3, 4L, 5.0f, 6.0);

  @Nested
  @DisplayName("Writer Chain Tests")
  class PrimitiveRoundTripTests {

    @Test
    @DisplayName("Test writer chain discovery")
    void testWriterChainDiscovery() {
      // Given a pickler for a record with primitive values
      final var pickler = new PicklerUsingAst<>(PrimitiveValueRecord.class);
      // When we have the boolean accessor
      final var methodHandle = pickler.componentAccessors[0][0];
      // And the boolean type expression
      final var typeExpr = pickler.componentTypeExpressions[0][0];
      final var writerChain = pickler.buildWriterChain(typeExpr, methodHandle);
      assertThat(writerChain).isNotNull();
      // We can write the record to a ByteBuffer
      final var byteBuffer = java.nio.ByteBuffer.allocate(1024);
      writerChain.accept(byteBuffer, primitiveValueRecord);
      byteBuffer.flip();
      // Then we create the reader chain for the same type expression
      final var readerChain = pickler.buildReaderChain(typeExpr);
      // we can read the record back from the ByteBuffer
      final boolean bool = (boolean) readerChain.apply(byteBuffer);
      assertThat(bool).isEqualTo(primitiveValueRecord.bool());
    }

  }
}
