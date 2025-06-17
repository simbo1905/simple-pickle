// SPDX-FileCopyrightText: 2025 Simon Massey
// SPDX-License-Identifier: Apache-2.0
//
package io.github.simbo1905.no.framework;

import org.junit.jupiter.api.*;

import java.lang.invoke.MethodHandle;
import java.lang.reflect.Type;

import static io.github.simbo1905.no.framework.Pickler.LOGGER;
import static org.assertj.core.api.Assertions.assertThat;

/// Package-private tests for core machinery components
/// Tests the internal implementation details that are not part of the public API
class MetaProgrammingTests {

  public static final Class[] EMPTY_PARAMETER_TYPES = {};
  static PrimitiveValueRecord primitiveValueRecord =
      new PrimitiveValueRecord(true, (byte) 1, 'a', (short) 2, 3, 4L, 5.0f, 6.0);
  int[] anIntNotZero = new int[]{42};

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

  public int anIntNotZero() {
    return anIntNotZero[0];
  }

  @Nested
  @DisplayName("Writer Chain Tests")
  class PrimitiveRoundTripTests {

    @Test
    @DisplayName("Test writer chain discovery")
    void testWriterChainDiscovery() throws Exception {

      Type type = MetaProgrammingTests.class.getDeclaredField("anIntNotZero").getGenericType();
      TypeExpr node = TypeExpr.analyze(type);
      TypeExpr.PrimitiveValueNode primitiveValueNode = (TypeExpr.PrimitiveValueNode) node;
      TypeExpr.PrimitiveValueType typeExpr = primitiveValueNode.type();
      MethodHandle methodHandle = MetaProgrammingTests.class.getMethod("anIntNotZero", EMPTY_PARAMETER_TYPES);
      final var writerChain = PicklerUsingAst.buildPrimitiveValueWriter(typeExpr, methodHandle);
      assertThat(writerChain).isNotNull();
      // We can write the record to a ByteBuffer
      final var byteBuffer = java.nio.ByteBuffer.allocate(1024);
      writerChain.accept(byteBuffer, primitiveValueRecord);
      byteBuffer.flip();
    }

  }
}
