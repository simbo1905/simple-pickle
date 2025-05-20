package io.github.simbo1905.no.framework;

import java.lang.reflect.Array;
import java.lang.reflect.RecordComponent;
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
    final var result = new RecordPickler<>(recordClass, new InternedName(name), classesByShortName);
    REGISTRY.putIfAbsent(recordClass, result);
    return result;
  }

  static <R extends Record> Pickler<R> manufactureRecordPickler(final Class<R> recordClass) {
    return manufactureRecordPickler(new HashMap<>(), recordClass, recordClass.getName());
  }

  static int write(ByteBuffer buffer, int value) {
    if (ZigZagEncoding.sizeOf(value) < Integer.BYTES) {
      LOGGER.finer(() -> "write(varint) - Enter: value=" + value + " position=" + buffer.position() + " size=" + ZigZagEncoding.sizeOf(value));
      buffer.put(INTEGER_VAR.marker());
      return 1 + ZigZagEncoding.putInt(buffer, value);
    } else {
      LOGGER.finer(() -> "write(int) - Enter: value=" + value + " position=" + buffer.position());
      buffer.put(INTEGER.marker());
      buffer.putInt(value);
      return 1 + Integer.BYTES;
    }
  }

  static int write(ByteBuffer buffer, long value) {
    if (ZigZagEncoding.sizeOf(value) < Long.BYTES) {
      LOGGER.finer(() -> "write(varlong) - Enter: value=" + value + " position=" + buffer.position() + " size=" + ZigZagEncoding.sizeOf(value));
      buffer.put(LONG_VAR.marker());
      return 1 + ZigZagEncoding.putLong(buffer, value);
    } else {
      LOGGER.finer(() -> "write(long) - Enter: value=" + value + " position=" + buffer.position());
      buffer.put(LONG.marker());
      buffer.putLong(value);
      return 1 + Long.BYTES;
    }
  }

  static int write(ByteBuffer buffer, double value) {
    LOGGER.finer(() -> "write(double) - Enter: value=" + value + " position=" + buffer.position());
    buffer.put(DOUBLE.marker());
    buffer.putDouble(value);
    return 1 + Double.BYTES;
  }

  static int write(ByteBuffer buffer, float value) {
    LOGGER.finer(() -> "write(float) - Enter: value=" + value + " position=" + buffer.position());
    buffer.put(FLOAT.marker());
    buffer.putFloat(value);
    return 1 + Float.BYTES;
  }

  static int write(ByteBuffer buffer, short value) {
    Optional.ofNullable(buffer).ifPresent(b -> {
      LOGGER.finer(() -> "write(short) - Enter: value=" + value + " position=" + buffer.position());
      buffer.put(SHORT.marker());
      buffer.putShort(value);
    });
    return 1 + Short.BYTES;
  }

  static Object read(final Map<String, Class<?>> classesByShortName, final ByteBuffer buffer) {
    final byte marker = buffer.get();
    return switch (fromMarker(marker)) {
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
      case OPTIONAL_EMPTY -> {
        LOGGER.finer(() -> "read(empty) - position=" + buffer.position());
        yield Optional.empty();
      }
      case OPTIONAL_OF -> {
        LOGGER.finer(() -> "read(of) - position=" + buffer.position());
        yield Optional.ofNullable(read(classesByShortName, buffer));
      }
      case INTERNED_NAME -> {
        LOGGER.finer(() -> "read(name) - position=" + buffer.position());
        yield readInternedName(buffer);
      }
      case ENUM -> {
        LOGGER.finer(() -> "read(enum) - position=" + buffer.position());
        final var internedName = (InternedName) read(classesByShortName, buffer);
        final Class<?> componentType = classesByShortName.get(Objects.requireNonNull(internedName).name());
        assert componentType != null : "Component type not found for name: " + internedName.name();
        yield readInternedName(buffer);
      }
      case INTERNED_OFFSET -> {
        final int oldPosition = buffer.position();
        final int offset = buffer.getInt();
        final int highWaterMark = buffer.position();
        final int newPosition = buffer.position() + offset - 2;
        buffer.position(newPosition);
        int length = ZigZagEncoding.getInt(buffer);
        byte[] bytes = new byte[length];
        buffer.get(bytes);
        buffer.position(highWaterMark);
        LOGGER.finer(() -> "read(offset) - location=" + highWaterMark + " position=" + oldPosition);
        yield new InternedName(new String(bytes, StandardCharsets.UTF_8));
      }
      case INTERNED_OFFSET_VAR -> {
        final int oldPosition = buffer.position();
        final int highWaterMark = buffer.position();
        final int offset = ZigZagEncoding.getInt(buffer);
        buffer.position(buffer.position() + offset - 1);
        int length = ZigZagEncoding.getInt(buffer);
        byte[] bytes = new byte[length];
        buffer.get(bytes);
        buffer.position(highWaterMark);
        LOGGER.finer(() -> "read(offset) - location=" + highWaterMark + " position=" + oldPosition);
        yield new InternedName(new String(bytes, StandardCharsets.UTF_8));
      }
      case ARRAY -> {
        LOGGER.finer(() -> "read() - ARRAY start position=" + buffer.position());
        final var internedName = (InternedName) read(classesByShortName, buffer);
        final Class<?> componentType = classesByShortName.get(Objects.requireNonNull(internedName).name());
        assert componentType != null : "Component type not found for name: " + internedName.name();
        final int length = ZigZagEncoding.getInt(buffer);
        final Object array = Array.newInstance(componentType, length);
        if (componentType.equals(byte.class)) {
          buffer.get((byte[]) array);
        } else {
          // Deserialize each element using IntStream instead of for loop
          IntStream.range(0, length)
              .forEach(i -> Array.set(array, i, read(classesByShortName, buffer)));
        }
        yield array;
      }
      case SAME_TYPE, RECORD ->
          throw new IllegalArgumentException("This should not be reached as caller should call self or delegate to another pickler.");
      case MAP -> throw new AssertionError("not implemented MAP");
      case LIST -> throw new AssertionError("not implemented LIST");
    };
  }

  private static InternedName readInternedName(ByteBuffer buffer) {
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
  static void serializeWithPickler(PackedBuf buf, Pickler<?> pickler, Object object) {
    // Since we know at runtime that pickler is a RecordPickler and object is the right type
    ((RecordPickler) pickler).serializeWithMap(buf, (Record) object, true);
  }

  /// This method cannot be inlined as it is required as a type witness to allow the compiler to downcast the pickler
  static int sizeOf(Pickler<?> pickler, Object object) {
    //noinspection unchecked,rawtypes
    return ((RecordPickler) pickler).sizeOf((Record) object);
  }
}
