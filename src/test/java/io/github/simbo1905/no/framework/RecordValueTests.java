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
import java.util.Map;
import java.util.stream.IntStream;

import static io.github.simbo1905.no.framework.Pickler.LOGGER;
import static org.assertj.core.api.Assertions.assertThat;

public class RecordValueTests {
  // Sealed interface that permits both record and enum - LinkedList example
  public sealed interface Link permits LinkedRecord, LinkEnd {
  }

  public record LinkedRecord(int value, Link next) implements Link {
  }

  public enum LinkEnd implements Link {END}

  static Map<Class<?>, PicklerImpl.TypeInfo> classTypeInfoMap = Map.of(
      LinkEnd.class, new PicklerImpl.TypeInfo(1, 1L, LinkEnd.values()),
      LinkedRecord.class, new PicklerImpl.TypeInfo(2, 2L, null)
  );

  static Link linkedListTwoNodes = new LinkedRecord(1, new LinkedRecord(2, LinkEnd.END));

  @BeforeAll
  static void setupLogging() {
    io.github.simbo1905.LoggingControl.setupCleanLogging();
  }

  @BeforeEach
  void setUp() {
    LOGGER.fine(() -> "Starting RecordValueTests test");
  }

  @AfterEach
  void tearDown() {
    LOGGER.fine(() -> "Finished RecordValueTests test");
  }

  @Test
  @DisplayName("Test reference value round trips")
  void testRecordValues() {
    final @NotNull RecordComponent[] components = LinkedRecord.class.getRecordComponents();
    final @NotNull MethodHandle[] accessors = new MethodHandle[components.length];
    final @NotNull TypeExpr[] typeExprs = new TypeExpr[components.length];

    IntStream.range(0, components.length).forEach(i -> {
      RecordComponent component = components[i];
      try {
        MethodHandle accessor = MethodHandles.lookup().unreflect(component.getAccessor());
        accessors[i] = accessor;
        Type type = component.getGenericType();
        typeExprs[i] = TypeExpr.analyze(type);
      } catch (IllegalAccessException e) {
        throw new RuntimeException("Failed to unreflect accessor for " + component.getName(), e);
      }
    });

    // we are ignoring the int and going for the interface type
    testInterfaceRoundTrip(typeExprs[1], accessors[1]);
  }

  void testInterfaceRoundTrip(TypeExpr typeExpr, MethodHandle accessor) {
    LOGGER.fine(() -> "Type of Record component: " + typeExpr);
    assertThat(typeExpr.isUserType()).isTrue();

    if (typeExpr instanceof TypeExpr.RefValueNode node) {
      LOGGER.fine("Component is Record");
      assertThat(node.type()).isEqualTo(TypeExpr.RefValueType.INTERFACE);

      final var writer = PicklerUsingAst.buildRecordWriter(classTypeInfoMap, accessor);
      assertThat(writer).isNotNull();

      final var buffer = ByteBuffer.allocate(1024);
      LOGGER.fine("Attempting to write Record to buffer");

      try {
        writer.accept(buffer, linkedListTwoNodes);
      } catch (Throwable e) {
        LOGGER.severe("Failed to write Record: " + e.getMessage());
        throw new RuntimeException(e.getMessage(), e);
      }

      buffer.flip();
      LOGGER.fine("Successfully wrote Record to buffer");

      final var reader = PicklerUsingAst.buildRecordReader(classTypeInfoMap);
      final Link result = (Link) reader.apply(buffer);

      LOGGER.fine("Read Record: " + result);
      assertThat(result).isEqualTo(linkedListTwoNodes);

      final int bytesWritten = buffer.position();
      final var sizer = PicklerUsingAst.buildRecordSizer(node.type(), accessor);
      final int size = sizer.applyAsInt(linkedListTwoNodes);

      LOGGER.fine("Bytes written: " + bytesWritten + ", Sizer returned: " + size);
      assertThat(size).isGreaterThanOrEqualTo(bytesWritten);
    } else {
      throw new IllegalStateException("Unexpected value: " + typeExpr);
    }
  }
}
