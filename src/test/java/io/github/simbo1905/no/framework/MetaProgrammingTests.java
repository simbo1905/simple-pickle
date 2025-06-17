// SPDX-FileCopyrightText: 2025 Simon Massey
// SPDX-License-Identifier: Apache-2.0
//
package io.github.simbo1905.no.framework;

import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.*;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.RecordComponent;
import java.lang.reflect.Type;
import java.nio.ByteBuffer;
import java.util.stream.IntStream;

import static io.github.simbo1905.no.framework.Pickler.LOGGER;
import static org.assertj.core.api.Assertions.assertThat;

/// Package-private tests for core machinery components
/// Tests the internal implementation details that are not part of the public API
public class MetaProgrammingTests {

  public static final Class<?>[] EMPTY_PARAMETER_TYPES = {};
  static PrimitiveValueRecord primitiveValueRecord =
      new PrimitiveValueRecord(true, (byte) 1, 'a', (short) 2, 3, 4L, 5.0f, 6.0);

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

  @Nested
  @DisplayName("Writer Chain Tests")
  class PrimitiveRoundTripTest {

    @Test
    @DisplayName("Test primitive chains")
    void testPrimitives() {
      // Get the method handle for anIntNotZero()
      final @NotNull RecordComponent[] components = PrimitiveValueRecord.class.getRecordComponents();
      // make an array of the component accessor methods
      final @NotNull MethodHandle[] accessors = new MethodHandle[components.length];
      final @NotNull TypeExpr[] typeExprs = new TypeExpr[components.length];

      IntStream.range(0, components.length).forEach(i -> {
        RecordComponent component = components[i];

        // Get accessor method handle
        try {
          MethodHandle accessor = MethodHandles.lookup().unreflect(component.getAccessor());

          // Log the method handle details
          LOGGER.fine(() -> String.format("Accessor method handle %d: %s for component %s",
              i, accessor.type(), component.getName()));

          accessors[i] = accessor;
        } catch (IllegalAccessException e) {
          throw new RuntimeException("Failed to un reflect accessor for " + component.getName(), e);
        }
        // Analyze the type of the component
        Type type = component.getGenericType();
        typeExprs[i] = TypeExpr.analyze(type);
      });

      // Test boolean component
      final TypeExpr typeExpr0 = typeExprs[0];
      final MethodHandle typeExpr0Accessor = accessors[0];
      testBooleanRoundTrip(typeExpr0, typeExpr0Accessor);

      // Test byte component
      final TypeExpr typeExpr1 = typeExprs[1];
      final MethodHandle typeExpr1Accessor = accessors[1];
      testByteRoundTrip(typeExpr1, typeExpr1Accessor);
    }

    void testBooleanRoundTrip(TypeExpr typeExpr0, MethodHandle typeExpr0Accessor) {
      LOGGER.fine(() -> "Type of first component: " + typeExpr0);
      // We expect the first component to be a primitive type of boolean
      assertThat(typeExpr0.isPrimitive()).isTrue();
      // switch on it being boolean
      if (typeExpr0 instanceof TypeExpr.PrimitiveValueNode e) {
        LOGGER.fine(() -> "First component is boolean");
        assertThat(e.type()).isEqualTo(TypeExpr.PrimitiveValueType.BOOLEAN);
        // boolean.class
        final var writerChain = PicklerUsingAst.buildPrimitiveValueWriter(e.type(), typeExpr0Accessor);
        assertThat(writerChain).isNotNull();
        // We can write the record to a ByteBuffer
        final var byteBuffer = ByteBuffer.allocate(1024);
        LOGGER.fine(() -> "Attempting to write boolean value to buffer");
        try {
          writerChain.accept(byteBuffer, primitiveValueRecord);
        } catch (Throwable e2) {
          LOGGER.severe(() -> "Failed to write boolean value: " + e2.getMessage());
          throw new RuntimeException(e2);
        }
        byteBuffer.flip();
        LOGGER.fine(() -> "Successfully wrote boolean value to buffer");
        // Now we can read it back
        final var readerChain = PicklerUsingAst.buildPrimitiveValueReader(e.type());
        final var readValue = readerChain.apply(byteBuffer);
        LOGGER.fine(() -> "Read boolean value: " + readValue);
        // Check the value is as expected
        assertThat(readValue).isEqualTo(primitiveValueRecord.booleanValue());
        // check how much was written
        final int bytesWritten = byteBuffer.position();
        // check that the sizer will return the something greater than or equal to the bytes written
        final var sizer = PicklerUsingAst.buildPrimitiveValueSizer(e.type(), typeExpr0Accessor);
        final int size = sizer.applyAsInt(primitiveValueRecord);
        LOGGER.fine(() -> "Bytes written: " + bytesWritten + ", Sizer returned: " + size);
        assertThat(size).isGreaterThanOrEqualTo(bytesWritten);
      } else {
        throw new IllegalStateException("Unexpected value: " + typeExpr0);
      }
    }
  }

  void testByteRoundTrip(TypeExpr typeExpr, MethodHandle accessor) {
    LOGGER.fine(() -> "Type of byte component: " + typeExpr);
    // We expect the component to be a primitive type of byte
    assertThat(typeExpr.isPrimitive()).isTrue();
    // switch on it being byte
    if (typeExpr instanceof TypeExpr.PrimitiveValueNode e) {
      LOGGER.fine(() -> "Byte component is byte");
      assertThat(e.type()).isEqualTo(TypeExpr.PrimitiveValueType.BYTE);
      // byte.class
      final var writerChain = PicklerUsingAst.buildPrimitiveValueWriter(e.type(), accessor);
      assertThat(writerChain).isNotNull();
      // We can write the record to a ByteBuffer
      final var byteBuffer = ByteBuffer.allocate(1024);
      LOGGER.fine(() -> "Attempting to write byte value to buffer");
      try {
        writerChain.accept(byteBuffer, primitiveValueRecord);
      } catch (Throwable e2) {
        LOGGER.severe(() -> "Failed to write byte value: " + e2.getMessage());
        throw new RuntimeException(e2);
      }
      byteBuffer.flip();
      LOGGER.fine(() -> "Successfully wrote byte value to buffer");
      // Now we can read it back
      final var readerChain = PicklerUsingAst.buildPrimitiveValueReader(e.type());
      final var readValue = readerChain.apply(byteBuffer);
      LOGGER.fine(() -> "Read byte value: " + readValue);
      // Check the value is as expected
      assertThat(readValue).isEqualTo(primitiveValueRecord.byteValue());
      // check how much was written
      final int bytesWritten = byteBuffer.position();
      // check that the sizer will return the something greater than or equal to the bytes written
      final var sizer = PicklerUsingAst.buildPrimitiveValueSizer(e.type(), accessor);
      final int size = sizer.applyAsInt(primitiveValueRecord);
      LOGGER.fine(() -> "Bytes written: " + bytesWritten + ", Sizer returned: " + size);
      assertThat(size).isGreaterThanOrEqualTo(bytesWritten);
    } else {
      throw new IllegalStateException("Unexpected value: " + typeExpr);
    }
  }
}
