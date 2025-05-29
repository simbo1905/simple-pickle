// SPDX-FileCopyrightText: 2025 Simon Massey
// SPDX-License-Identifier: MIT

package io.github.simbo1905.no.framework;

import java.lang.reflect.Array;
import java.lang.reflect.RecordComponent;
import java.lang.reflect.Type;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static io.github.simbo1905.no.framework.Constants.*;
import static io.github.simbo1905.no.framework.Pickler.LOGGER;

class Companion {
  /// We cache the picklers for each class to avoid creating them multiple times
  static ConcurrentHashMap<Class<?>, Pickler<?>> REGISTRY = new ConcurrentHashMap<>();

  ///  Here we are typing things as `Record` to avoid the need for a cast
  static <R extends Record> Pickler<R> manufactureRecordPickler(
      final Map<String, Class<?>> classesByShortName,
      final Class<R> recordClass,
      final String name) {
    Objects.requireNonNull(recordClass);
    Objects.requireNonNull(name);
    final var result = new RecordPickler<>(recordClass);
    REGISTRY.putIfAbsent(recordClass, result);
    return result;
  }

  static <R extends Record> Pickler<R> manufactureRecordPickler(final Class<R> recordClass) {
    return manufactureRecordPickler(new HashMap<>(), recordClass, recordClass.getName());
  }

  static Object read(final int componentIndex, final ReadBufferImpl buf) {
    final var buffer = buf.buffer;
    final int position = buffer.position();
    final byte marker = buffer.get();
    final Constants type = fromMarker(marker);
    if (type == null) {
      throw new IllegalArgumentException("Unknown marker: " + marker + " at position: " + position);
    }
    return switch (type) {
      case NULL -> {
        LOGGER.finer(() -> "read(NULL) - position=" + buffer.position());
        yield null;
      }
      case BOOLEAN -> {
        final boolean value = buffer.get() != 0x0;
        LOGGER.finer(() -> "read(bool) - " + value + " position=" + buffer.position());
        yield value;
      }
      case BYTE -> {
        final byte value = buffer.get();
        LOGGER.finer(() -> "read(byte) - " + value + " position=" + buffer.position());
        yield value;
      }
      case SHORT -> {
        final short value = buffer.getShort();
        LOGGER.finer(() -> "read(short) - " + value + " position=" + buffer.position());
        yield value;
      }
      case CHARACTER -> {
        final char value = buffer.getChar();
        LOGGER.finer(() -> "read(char) - " + value + " position=" + buffer.position());
        yield value;
      }
      case INTEGER -> {
        final int value = buffer.getInt();
        LOGGER.finer(() -> "read(int) - " + value + " position=" + buffer.position());
        yield value;
      }
      case INTEGER_VAR -> {
        final int value = ZigZagEncoding.getInt(buffer);
        LOGGER.finer(() -> "read(varint) - " + value + " position=" + buffer.position());
        yield value;
      }
      case LONG -> {
        final long value = buffer.getLong();
        LOGGER.finer(() -> "read(long) - " + value + " position=" + buffer.position());
        yield value;
      }
      case LONG_VAR -> {
        final long value = ZigZagEncoding.getLong(buffer);
        LOGGER.finer(() -> "read(varlong) - " + value + " position=" + buffer.position());
        yield value;
      }
      case FLOAT -> {
        final float value = buffer.getFloat();
        LOGGER.finer(() -> "read(float) - " + value + " position=" + buffer.position());
        yield value;
      }
      case DOUBLE -> {
        final double value = buffer.getDouble();
        LOGGER.finer(() -> "read(double) - " + value + " position=" + buffer.position());
        yield value;
      }
      case STRING -> {
        int length = ZigZagEncoding.getInt(buffer);
        LOGGER.finer(() -> "read(String) - start position=" + buffer.position() + " length=" + ZigZagEncoding.sizeOf(length));
        byte[] bytes = new byte[length];
        buffer.get(bytes);
        yield new String(bytes, StandardCharsets.UTF_8);
      }
      case UUID -> {
        LOGGER.finer(() -> "read(UUID) - start position=" + buffer.position());
        long mostSigBits = buffer.getLong();
        long leastSigBits = buffer.getLong();
        yield new java.util.UUID(mostSigBits, leastSigBits);
      }
      case OPTIONAL_EMPTY -> {
        LOGGER.finer(() -> "read(empty) - position=" + buffer.position());
        yield Optional.empty();
      }
      case OPTIONAL_OF -> {
        LOGGER.finer(() -> "read(of) - position=" + buffer.position());
        yield Optional.ofNullable(read(componentIndex, buf));
      }
      case INTERNED_NAME -> {
        LOGGER.finer(() -> "read(name) - position=" + buffer.position());
        yield unintern(buffer);
      }
      case INTERNED_OFFSET -> {
        // Resolve and return actual interned name from the offset
        final int offset = buffer.getInt();
        final int highWaterMark = buffer.position();
        final int lowWaterMark = buffer.position() + offset - 4;
        buffer.position(lowWaterMark);
        final var uninterned = unintern(buffer);
        buffer.position(highWaterMark);
        LOGGER.finer(() -> "read(offset) - lowWaterMark=" + lowWaterMark + " position=" + highWaterMark);
        yield uninterned;
      }
      case INTERNED_OFFSET_VAR -> {
        final int highWaterMark = buffer.position();
        final int offset = ZigZagEncoding.getInt(buffer);
        final int lowWaterMark = buffer.position() + offset - 1;
        buffer.position(lowWaterMark);
        final var uninterned = unintern(buffer);
        buffer.position(highWaterMark);
        LOGGER.finer(() -> "read(offset_var) - lowWaterMark=" + lowWaterMark + " position=" + highWaterMark);
        yield uninterned;
      }
      case ARRAY -> {
        LOGGER.finer(() -> "read(ARRAY) start position=" + buffer.position());
        final var internedName = (InternedName) read(componentIndex, buf);
        final Class<?> componentType = buf.nameToClass.get(Objects.requireNonNull(internedName).name());
        assert componentType != null : "Component type not found for name: " + internedName.name();
        final int length = ZigZagEncoding.getInt(buffer);
        final Object array = Array.newInstance(componentType, length);
        if (componentType.equals(byte.class)) {
          buffer.get((byte[]) array);
        } else {
          // Deserialize each element using IntStream instead of for loop
          IntStream.range(0, length)
              .forEach(i -> Array.set(array, i, read(componentIndex, buf)));
        }
        yield array;
      }
      case ENUM -> {
        final var locatorPosition = buffer.position();
        final var locator = Companion.read(componentIndex, buf);
        LOGGER.finer(() -> "read(enum) -" +
            " position=" + buffer.position() +
            " locator=" + locator
        );
        if (locator instanceof InternedName(final var name)) {
          assert buf.nameToEnum.containsKey(name) : "Name not found in enum map: " + name;
          yield buf.nameToEnum.get(name);
        }
        Objects.requireNonNull(locator, "enum location cannot be null");
        throw new IllegalStateException("invalid enum location type found at position=" + locatorPosition + " + locator=" + locator);
      }
      case LIST -> {
        LOGGER.finer(() -> "read(list) - start position=" + buffer.position());
        final var internedName = (InternedName) read(componentIndex, buf);
        final Type[] componentTypes = buf.componentGenericTypes.get(componentIndex);
        assert componentTypes != null && componentTypes.length == 1 : "componentGenericTypes must contain at least one type";
        final Class<?> componentType = buf.nameToClass.get(Objects.requireNonNull(internedName).name());
        assert componentType != null : "Component type not found for name: " + internedName.name();
        final int size = ZigZagEncoding.getInt(buffer);
        LOGGER.finer(() -> "read(list) - size=" + size + " position=" + buffer.position());
        if (size == 0) {
          yield Collections.emptyList();
        }
        final List<Object> list = new ArrayList<>(size);
        IntStream.range(0, size).forEach(i -> {
          final Object item = read(componentIndex, buf);
          if (item == null) {
            throw new IllegalStateException("List item cannot be null at index " + i + " in list of type " + componentType.getName());
          }
          if (!componentType.isInstance(item)) {
            throw new IllegalStateException("List item at index " + i + " is not of type " + componentType.getName() + ": " + item.getClass().getName());
          }
          list.add(i, item);
        });
        // Return an immutable list to prevent modification
        yield Collections.unmodifiableList(list);
      }
      case SAME_TYPE, RECORD ->
          throw new IllegalArgumentException("This should not be reached as caller should call self or delegate to another pickler.");
      case MAP -> throw new AssertionError("not implemented MAP");
    };
  }

  static InternedName unintern(ByteBuffer buffer) {
    int length = ZigZagEncoding.getInt(buffer);
    byte[] bytes = new byte[length];
    buffer.get(bytes);
    return new InternedName(new String(bytes, StandardCharsets.UTF_8));
  }

  /// Implementation that traverses the hierarchy using a visited set to avoid cycles.
  ///
  /// @param current the current class being processed
  /// @param visited set of already visited classes
  /// @return stream of classes in the hierarchy
  static Stream<Class<?>> recordClassHierarchy(
      final Class<?> current,
      final Set<Class<?>> visited
  ) {
    if (!visited.add(current)) {
      return Stream.empty();
    }

    return Stream.concat(
        Stream.of(current),
        Stream.concat(
            current.isSealed()
                ? Arrays.stream(current.getPermittedSubclasses())
                : Stream.empty(),

            current.isRecord()
                ? Arrays.stream(current.getRecordComponents())
                .map(RecordComponent::getType)
                .filter(t -> t.isRecord() || t.isSealed())
                : Stream.empty()
        ).flatMap(child -> recordClassHierarchy(child, visited))
    );
  }

  static Map<String, Class<?>> nameToBasicClass = Map.of(
      "byte", byte.class,
      "short", short.class,
      "char", char.class,
      "int", int.class,
      "long", long.class,
      "float", float.class,
      "double", double.class
  );

  /// This method cannot be inlined as it is required as a type witness to allow the compiler to downcast the pickler
  @SuppressWarnings({"unchecked", "rawtypes"})
  static void serializeWithPickler(WriteBufferImpl buf, Pickler<?> pickler, Object object) {
    // Since we know at runtime that pickler is a RecordPickler and object is the right type
    //((RecordPickler) pickler).serializeWithMap(buf, (Record) object, true);
  }

  /// This method cannot be inlined as it is required as a type witness to allow the compiler to downcast the pickler
  @SuppressWarnings({"unchecked", "rawtypes"})
  static int sizeOfWithPickler(Pickler<?> pickler, Object object) {
    // Since we know at runtime that pickler is a RecordPickler and object is the right type
    return ((RecordPickler) pickler).reflection.maxSize((Record) object);
  }

}
