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
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import static io.github.simbo1905.no.framework.Pickler.LOGGER;
import static org.assertj.core.api.Assertions.assertThat;

public class RecordValueTests {

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
  public enum LinkEnd implements Link {END}

  // TypeInfo for the classes used in the tests
  static Map<Class<?>, PicklerImpl.TypeInfo> classTypeInfoMap = Map.of(
      LinkEnd.class, new PicklerImpl.TypeInfo(1, 1L, LinkEnd.values(), null),
      LinkedRecord.class, new PicklerImpl.TypeInfo(2, 2L, null, createLikedRecordInnerWriters()),
      Boxed.class, new PicklerImpl.TypeInfo(3, 3L, null, createBoxedRecordInnerWriters())
  );

  static Supplier<BiConsumer<ByteBuffer, Object>[]> createBoxedRecordInnerWriters() {
    return () -> {
      throw new AssertionError("createBoxedRecordInnerWriters() not implemented");
    };
  }

  static Supplier<BiConsumer<ByteBuffer, Object>[]> createLikedRecordInnerWriters() {
    return () -> {
      throw new AssertionError("createLikedRecordInnerWriters() not implemented");
    };
  }

  // A linked list with two nodes to test with
  static Link linkedListTwoNodes = new LinkedRecord(new Boxed(1), new LinkedRecord(new Boxed(2), LinkEnd.END));

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
  @DisplayName("Test record value round trips")
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

    testRecordRoundTrip(typeExprs[0], accessors[0], Boxed.class);
    testInterfaceRoundTrip(typeExprs[1], accessors[1]);
  }

  void testRecordRoundTrip(TypeExpr typeExpr, MethodHandle accessor, Class<?> innerRecordClass) {
    LOGGER.fine(() -> "Type of Record component: " + typeExpr);
    assertThat(typeExpr.isUserType()).isTrue();

    if (typeExpr instanceof TypeExpr.RefValueNode node) {
      LOGGER.fine("Component is Record");
      assertThat(node.type()).isEqualTo(TypeExpr.RefValueType.RECORD);
      final var typeInfo = classTypeInfoMap.get(innerRecordClass);
      final var writer = PicklerUsingAst.buildRecordWriter(typeInfo, accessor);
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

      final var innerTypeInfo = classTypeInfoMap.get(innerRecordClass);
      final var reader = PicklerUsingAst.buildRecordReader(typeInfo);
      final Link result = (Link) reader.apply(buffer);

      LOGGER.fine("Read Record: " + result);
      assertThat(result).isEqualTo(linkedListTwoNodes);

      final int bytesWritten = buffer.position();
      final var sizer = PicklerUsingAst.buildRecordSizer(typeInfo, node.type(), accessor);
      final int size = sizer.applyAsInt(linkedListTwoNodes);

      LOGGER.fine("Bytes written: " + bytesWritten + ", Sizer returned: " + size);
      assertThat(size).isGreaterThanOrEqualTo(bytesWritten);
    } else {
      throw new IllegalStateException("Unexpected value: " + typeExpr);
    }
  }

  void testInterfaceRoundTrip(TypeExpr typeExpr, MethodHandle accessor) {
    LOGGER.fine(() -> "Type of Record component: " + typeExpr);
    assertThat(typeExpr.isUserType()).isTrue();

    if (typeExpr instanceof TypeExpr.RefValueNode node) {
      LOGGER.fine("Component is Record");
      assertThat(node.type()).isEqualTo(TypeExpr.RefValueType.INTERFACE);
      final var typeInfo = classTypeInfoMap.get(LinkedRecord.class);
      final var writer = PicklerUsingAst.buildRecordWriter(typeInfo, accessor);
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

      final var reader = PicklerUsingAst.buildRecordReader(typeInfo);
      final Link result = (Link) reader.apply(buffer);

      LOGGER.fine("Read Record: " + result);
      assertThat(result).isEqualTo(linkedListTwoNodes);

      final int bytesWritten = buffer.position();
      final var sizer = PicklerUsingAst.buildRecordSizer(typeInfo, node.type(), accessor);
      final int size = sizer.applyAsInt(linkedListTwoNodes);

      LOGGER.fine("Bytes written: " + bytesWritten + ", Sizer returned: " + size);
      assertThat(size).isGreaterThanOrEqualTo(bytesWritten);
    } else {
      throw new IllegalStateException("Unexpected value: " + typeExpr);
    }
  }
}
