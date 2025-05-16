package io.github.simbo1905.no.framework;

import org.jetbrains.annotations.NotNull;

import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static io.github.simbo1905.no.framework.Constants.*;
import static io.github.simbo1905.no.framework.Pickler.LOGGER;

class Companion {
  /// We cache the picklers for each class to avoid creating them multiple times
  static ConcurrentHashMap<Class<?>, Pickler<?>> REGISTRY = new ConcurrentHashMap<>();

  /// Logic to create and cache picklers
  @SuppressWarnings("unchecked")
  static <T> Pickler<T> getOrCreate(Class<T> type, Supplier<Pickler<T>> supplier) {
    return (Pickler<T>) REGISTRY.computeIfAbsent(type, k -> supplier.get());
  }

  ///  Here we are typing things as `Record` to avoid the need for a cast
  static <R extends Record> Pickler<R> manufactureRecordPickler(final Class<R> recordClass, final String name) {
    return new RecordPickler<>(recordClass, name != null ? new InternedName(name) : null);
  }

  static int write(ByteBuffer buffer, int value) {
    LOGGER.finer(() -> "write(int) - Enter: value=" + value + " position=" + buffer.position());
    if (ZigZagEncoding.sizeOf(value) < Integer.BYTES) {
      buffer.put(Constants.INTEGER_VAR.marker());
      return 1 + ZigZagEncoding.putInt(buffer, value);
    } else {
      buffer.put(Constants.INTEGER.marker());
      buffer.putInt(value);
      return 1 + Integer.BYTES;
    }
  }

  static int write(ByteBuffer buffer, long value) {
    LOGGER.finer(() -> "write(long) - Enter: value=" + value + " position=" + buffer.position());
    if (ZigZagEncoding.sizeOf(value) < Long.BYTES) {
      buffer.put(LONG_VAR.marker());
      return 1 + ZigZagEncoding.putLong(buffer, value);
    } else {
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
    LOGGER.finer(() -> "write(short) - Enter: value=" + value + " position=" + buffer.position());
    buffer.put(SHORT.marker());
    buffer.putShort(value);
    return 1 + Short.BYTES;
  }

  static int write(ByteBuffer buffer, char value) {
    LOGGER.finer(() -> "write(char) - Enter: value=" + value + " position=" + buffer.position());
    buffer.put(CHARACTER.marker());
    buffer.putChar(value);
    return 1 + Character.BYTES;
  }

  static int write(ByteBuffer buffer, boolean value) {
    LOGGER.finer(() -> "write(boolean) - Enter: value=" + value + " position=" + buffer.position());
    buffer.put(BOOLEAN.marker());
    if (value) {
      buffer.put((byte) 1);
    } else {
      buffer.put((byte) 0);
    }
    return 1 + 1;
  }

  static int write(ByteBuffer buffer, String s) {
    Objects.requireNonNull(s);
    buffer.put(STRING.marker());
    byte[] utf8 = s.getBytes(StandardCharsets.UTF_8);
    int length = utf8.length;
    ZigZagEncoding.putInt(buffer, length);
    buffer.put(utf8);
    return 1 + length;
  }

  static int writeNull(ByteBuffer buffer) {
    buffer.put(NULL.marker());
    return 1;
  }

  static int write(ByteBuffer buffer, InternedName type) {
    Objects.requireNonNull(type);
    Objects.requireNonNull(type.name());
    buffer.put(INTERNED_NAME.marker());
    return 1 + intern(buffer, type.name());
  }

  static int intern(ByteBuffer buffer, String string) {
    final var nameBytes = string.getBytes(StandardCharsets.UTF_8);
    final var nameLength = nameBytes.length;
    var size = ZigZagEncoding.putInt(buffer, nameLength);
    buffer.put(nameBytes);
    size += nameLength;
    return size;
  }

  static <T extends Enum<?>> int write(final Map<InternedName, InternedPosition> offsetMap, ByteBuffer buffer, String ignoredPrefix, T e) {
    Objects.requireNonNull(e);
    Objects.requireNonNull(ignoredPrefix);
    final var className = e.getDeclaringClass().getName();
    final var shortName = className.substring(ignoredPrefix.length());
    final var dotName = shortName + "." + e.name();
    final var internedName = new InternedName(dotName);
    if (!offsetMap.containsKey(internedName)) {
      offsetMap.put(internedName, new InternedPosition(buffer.position()));
      buffer.put(ENUM.marker());
      return 1 + intern(buffer, dotName);
    } else {
      final var internedPosition = offsetMap.get(internedName);
      final var internedOffset = new InternedOffset(internedPosition.position() - buffer.position());
      return write(buffer, internedOffset);
    }
  }

  static int write(ByteBuffer buffer, InternedOffset typeOffset) {
    Objects.requireNonNull(typeOffset);
    final int offset = typeOffset.offset();
    final int size = ZigZagEncoding.sizeOf(offset);
    if (size < Integer.BYTES) {
      buffer.put(INTERNED_OFFSET_VAR.marker());
      return 1 + ZigZagEncoding.putInt(buffer, offset);
    } else {
      buffer.put(INTERNED_OFFSET.marker());
      buffer.putInt(offset);
      return 1 + Integer.BYTES;
    }
  }

  static <T> int write(ByteBuffer buffer, @SuppressWarnings("OptionalUsedAsFieldOrParameterType") Optional<T> optional) {
    if (optional.isPresent()) {
      buffer.put(OPTIONAL_OF.marker());
      final T value = optional.get();
      final int innerSize = switch (value) {
        case Integer i -> write(buffer, i);
        case Long l -> write(buffer, l);
        case Short s -> write(buffer, s);
        case Byte b -> write(buffer, b);
        case Double d -> write(buffer, d);
        case Float f -> write(buffer, f);
        case Character c -> write(buffer, c);
        case Boolean b -> write(buffer, b);
        case String s -> write(buffer, s);
        case Optional<?> o -> write(buffer, o);
        case InternedName t -> write(buffer, t);
        case InternedOffset t -> write(buffer, t);
        default -> throw new AssertionError("unknown optional value " + value);
      };
      return 1 + innerSize;
    } else {
      buffer.put(OPTIONAL_EMPTY.marker());
      return 1;
    }
  }

  static Object read(final Map<String, Class<?>> classesByShortName, final ByteBuffer buffer) {
    LOGGER.finer(() -> "read() - Enter: position=" + buffer.position());
    final byte marker = buffer.get();
    LOGGER.finer(() -> "read() - marker=" + marker + " position=" + buffer.position());
    return switch (fromMarker(marker)) {
      case NULL -> {
        LOGGER.finer(() -> "read() - NULL position=" + buffer.position());
        yield null;
      }
      case BOOLEAN -> {
        final boolean value = buffer.get() != 0x0;
        LOGGER.finer(() -> "read() - BOOLEAN: " + value + " position=" + buffer.position());
        yield value;
      }
      case BYTE -> {
        final byte value = buffer.get();
        LOGGER.finer(() -> "read() - BYTE: " + value + " position=" + buffer.position());
        yield value;
      }
      case SHORT -> {
        final short value = buffer.getShort();
        LOGGER.finer(() -> "read() - SHORT: " + value + " position=" + buffer.position());
        yield value;
      }
      case CHARACTER -> {
        final char value = buffer.getChar();
        LOGGER.finer(() -> "read() - CHARACTER: " + value + " position=" + buffer.position());
        yield value;
      }
      case INTEGER -> {
        final int value = buffer.getInt();
        LOGGER.finer(() -> "read() - INTEGER: " + value + " position=" + buffer.position());
        yield value;
      }
      case INTEGER_VAR -> {
        final int value = ZigZagEncoding.getInt(buffer);
        LOGGER.finer(() -> "read() - INTEGER_VAR: " + value + " position=" + buffer.position());
        yield value;
      }
      case LONG -> {
        final long value = buffer.getLong();
        LOGGER.finer(() -> "read() - LONG: " + value + " position=" + buffer.position());
        yield value;
      }
      case LONG_VAR -> {
        final long value = ZigZagEncoding.getLong(buffer);
        LOGGER.finer(() -> "read() - LONG_VAR: " + value + " position=" + buffer.position());
        yield value;
      }
      case FLOAT -> {
        final float value = buffer.getFloat();
        LOGGER.finer(() -> "read() - FLOAT: " + value + " position=" + buffer.position());
        yield value;
      }
      case DOUBLE -> {
        final double value = buffer.getDouble();
        LOGGER.finer(() -> "read() - DOUBLE: " + value + " position=" + buffer.position());
        yield value;
      }
      case STRING -> {
        LOGGER.finer(() -> "read() - STRING start position=" + buffer.position());
        int length = ZigZagEncoding.getInt(buffer);
        LOGGER.finer(() -> "read() - STRING length=" + length + " position=" + buffer.position());
        byte[] bytes = new byte[length];
        buffer.get(bytes);
        final String result = new String(bytes, StandardCharsets.UTF_8);
        LOGGER.finer(() -> "read() - STRING result=" + result + " position=" + buffer.position());
        yield result;
      }
      case OPTIONAL_EMPTY -> {
        LOGGER.finer(() -> "read() - OPTIONAL_EMPTY position=" + buffer.position());
        yield Optional.empty();
      }
      case OPTIONAL_OF -> {
        LOGGER.finer(() -> "read() - OPTIONAL_OF position=" + buffer.position());
        yield Optional.ofNullable(read(classesByShortName, buffer));
      }
      case INTERNED_NAME, ENUM -> {
        LOGGER.finer(() -> "read() - INTERNED_NAME/ENUM position=" + buffer.position());
        yield readInternedName(buffer);
      }
      case INTERNED_OFFSET -> {
        LOGGER.finer(() -> "read() - INTERNED_OFFSET start position=" + buffer.position());
        final int offset = buffer.getInt();
        final int highWaterMark = buffer.position();
        final int newPosition = buffer.position() + offset - 2;
        buffer.position(newPosition);
        LOGGER.finer(() -> "read() - INTERNED_OFFSET offset=" + offset + " position=" + newPosition);
        int length = ZigZagEncoding.getInt(buffer);
        LOGGER.finer(() -> "read() - INTERNED_OFFSET length=" + length + " position=" + buffer.position());
        byte[] bytes = new byte[length];
        buffer.get(bytes);
        LOGGER.finer(() -> "read() - INTERNED_OFFSET bytes read position=" + buffer.position());
        buffer.position(highWaterMark);
        LOGGER.finer(() -> "read() - INTERNED_OFFSET result position=" + highWaterMark);
        yield new InternedName(new String(bytes, StandardCharsets.UTF_8));
      }
      case INTERNED_OFFSET_VAR -> {
        LOGGER.finer(() -> "read() - INTERNED_OFFSET_VAR start position=" + buffer.position());
        final int highWaterMark = buffer.position();
        final int offset = ZigZagEncoding.getInt(buffer);
        LOGGER.finer(() -> "read() - INTERNED_OFFSET_VAR offset=" + offset + " position=" + buffer.position());
        buffer.position(buffer.position() + offset - 1);
        LOGGER.finer(() -> "read() - INTERNED_OFFSET_VAR new position=" + buffer.position());
        int length = ZigZagEncoding.getInt(buffer);
        LOGGER.finer(() -> "read() - INTERNED_OFFSET_VAR length=" + length + " position=" + buffer.position());
        byte[] bytes = new byte[length];
        buffer.get(bytes);
        LOGGER.finer(() -> "read() - INTERNED_OFFSET_VAR bytes read position=" + buffer.position());
        buffer.position(highWaterMark);
        LOGGER.finer(() -> "read() - INTERNED_OFFSET_VAR result position=" + highWaterMark);
        yield new InternedName(new String(bytes, StandardCharsets.UTF_8));
      }
      case ARRAY -> {
        LOGGER.finer(() -> "read() - ARRAY start position=" + buffer.position());
        final var internedName = (InternedName) read(classesByShortName, buffer);
        LOGGER.finer(() -> "read() - ARRAY internedName=" + Objects.requireNonNull(internedName).name() + " position=" + buffer.position());
        final Class<?> componentType = classesByShortName.get(Objects.requireNonNull(internedName).name());
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
      case MAP -> throw new AssertionError("not implemented 2");
      case LIST -> throw new AssertionError("not implemented 4");
    };
  }

  private static @NotNull InternedName readInternedName(ByteBuffer buffer) {
    int length = ZigZagEncoding.getInt(buffer);
    byte[] bytes = new byte[length];
    buffer.get(bytes);
    final var iname = new InternedName(new String(bytes, StandardCharsets.UTF_8));
    return iname;
  }

  /// Helper method to recursively find all permitted record classes
  /// @throws IllegalArgumentException if the class is not sealed or if any of the subclasses are not records or sealed interfaces
  static Stream<Class<?>> validateSealedRecordHierarchy(Class<?> sealedClass) {
    if (!sealedClass.isSealed()) {
      final var msg = "Class is not sealed: " + sealedClass.getName();
      LOGGER.severe(() -> msg);
      throw new IllegalArgumentException(msg);
    }

    return Arrays.stream(sealedClass.getPermittedSubclasses())
        .flatMap(subclass -> {
          if (subclass.isRecord()) {
            return Stream.of(subclass);
          } else if (subclass.isSealed()) {
            return validateSealedRecordHierarchy(subclass);
          } else {
            final var msg = "Permitted subclass must be either a record or sealed interface: " +
                subclass.getName();
            LOGGER.severe(() -> msg);
            throw new IllegalArgumentException(msg);
          }
        });
  }

  static Map<String, Class<?>> nameToBasicClass = Map.of(
      "byte", byte.class,
      "short", short.class,
      "char", char.class,
      "int", int.class,
      "long", long.class,
      "float", float.class,
      "double", double.class);
}
