// SPDX-FileCopyrightText: 2025 Simon Massey
// SPDX-License-Identifier: MIT

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

  /// Note that we do not varint encode a short as we would then need and additional byte to decode
  static int write(ByteBuffer buffer, short value) {
    LOGGER.finer(() -> "write(short) - Enter: value=" + value + " position=" + buffer.position());
    buffer.put(SHORT.marker());
    buffer.putShort(value);
    return 1 + Short.BYTES;
  }

  static Object read(
      final Map<String, Class<?>> classesByShortName, // FIXME: move this into the ReadBufferImpl
      final ReadBufferImpl buf) {
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
      case OPTIONAL_EMPTY -> {
        LOGGER.finer(() -> "read(empty) - position=" + buffer.position());
        yield Optional.empty();
      }
      case OPTIONAL_OF -> {
        LOGGER.finer(() -> "read(of) - position=" + buffer.position());
        yield Optional.ofNullable(read(classesByShortName, buf));
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
        final var internedName = (InternedName) read(classesByShortName, buf);
        final Class<?> componentType = classesByShortName.get(Objects.requireNonNull(internedName).name());
        assert componentType != null : "Component type not found for name: " + internedName.name();
        final int length = ZigZagEncoding.getInt(buffer);
        final Object array = Array.newInstance(componentType, length);
        if (componentType.equals(byte.class)) {
          buffer.get((byte[]) array);
        } else {
          // Deserialize each element using IntStream instead of for loop
          IntStream.range(0, length)
              .forEach(i -> Array.set(array, i, read(classesByShortName, buf)));
        }
        yield array;
      }
      case ENUM -> {
        final var locatorPosition = buffer.position();
        final var locator = Companion.read(classesByShortName, buf);
        LOGGER.finer(() -> "read(enum) - locator=" + locator + " - position=" + buffer.position());
        if (locator instanceof InternedName(final var name)) {
          assert buf.nameToEnum.containsKey(name) : "Name not found in enum map: " + name;
          yield buf.nameToEnum.get(name);
        }
        Objects.requireNonNull(locator, "enum location cannot be null");
        throw new IllegalStateException("invalid enum location type found at position=" + locatorPosition + " + locator=" + locator);
      }
      case SAME_TYPE, RECORD ->
          throw new IllegalArgumentException("This should not be reached as caller should call self or delegate to another pickler.");
      case MAP -> throw new AssertionError("not implemented MAP");
      case LIST -> throw new AssertionError("not implemented LIST");
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
  static int serializeWithPickler(WriteBufferImpl buf, Pickler<?> pickler, Object object) {
    // Since we know at runtime that pickler is a RecordPickler and object is the right type
    return ((RecordPickler) pickler).serializeWithMap(buf, (Record) object, true);
  }

  /// Writes types into a buffer recursively. This is used to write out the components of a record.
  /// In order to prevent infinite loops the caller of the method must look up the pickler of any
  /// inner records and delegate to that pickler. This method will throw an exception if it is called
  /// to write out a record that is not the specific type of the pickler.
  /// @param object the class of the record
  /// @throws IllegalStateException if the buffer is closed
  static int recursiveWrite(final WriteBufferImpl buf, Object object) {
    final var buffer = buf.buffer;
    return switch (object) {
      case null -> WriteBufferImpl.writeNull(buffer);
      case Integer i -> write(buffer, i);
      case Long l -> write(buffer, l);
      case Short s -> write(buffer, s);
      case Byte b -> WriteBufferImpl.write(buffer, b);
      case Double d -> write(buffer, d);
      case Float f -> write(buffer, f);
      case Character ch -> WriteBufferImpl.write(buffer, ch);
      case Boolean b -> WriteBufferImpl.write(buffer, b);
      case String s -> WriteBufferImpl.write(buffer, s);
      case InternedName t -> WriteBufferImpl.write(buffer, t);
      case InternedOffset t -> WriteBufferImpl.write(buffer, t);
      case Optional<?> o -> {
        int size = 1;
        if (o.isEmpty()) {
          LOGGER.finer(() -> "write(empty) - position=" + buffer.position());
          buffer.put(Constants.OPTIONAL_EMPTY.marker());
        } else {
          LOGGER.finer(() -> "write(optional) - position=" + buffer.position());
          buffer.put(Constants.OPTIONAL_OF.marker());
          size += recursiveWrite(buf, o.get());
        }
        yield size;
      }
      case List<?> l -> {
        LOGGER.finer(() -> "write(list) - size=" + ZigZagEncoding.sizeOf(l.size()) + " position=" + buffer.position());
        buffer.put(Constants.LIST.marker());
        int size = 1 + ZigZagEncoding.putInt(buffer, l.size());
        for (Object item : l) {
          size += recursiveWrite(buf, item);
        }
        yield size;
      }
      case Map<?, ?> m -> {
        LOGGER.finer(() -> "write(map) - size=" + ZigZagEncoding.sizeOf(m.size()) + " position=" + buffer.position());
        buffer.put(Constants.MAP.marker());
        int size = 1 + ZigZagEncoding.putInt(buffer, m.size());
        for (Map.Entry<?, ?> entry : m.entrySet()) {
          size += recursiveWrite(buf, entry.getKey());
          size += recursiveWrite(buf, entry.getValue());
        }
        yield size;
      }
      case Enum<?> e -> {
        LOGGER.finer(() -> "write(enum) - enumToName=" + buf.enumToName.get(e) + " position=" + buffer.position());
        buf.buffer.put(Constants.ENUM.marker());
        InternedName name = buf.enumToName.get(e);
        if (buf.offsetMap.containsKey(name)) {
          final var internedPosition = buf.offsetMap.get(name);
          final var internedOffset = internedPosition.offset(buffer.position());
          yield Companion.recursiveWrite(buf, internedOffset);
        } else {
          buf.offsetMap.computeIfAbsent(name, (x) -> new InternedPosition(buffer.position()));
          yield Companion.recursiveWrite(buf, name);
        }
      }
      // TODO zigzag compress long[] and int[]
      case Object a when a.getClass().isArray() -> {
        LOGGER.finer(() -> "write(array) - size=" + ZigZagEncoding.sizeOf(Array.getLength(a)) + " position=" + buffer.position());
        buffer.put(Constants.ARRAY.marker());
        // FIXME: this reflection should be done at pickler creation time
        final var InternedName = new InternedName(a.getClass().getComponentType().getName());
        int size = 1;
        // FIXME: this should write the interned name of an interned offset
        size += recursiveWrite(buf, InternedName);
        int length = Array.getLength(a);
        size += ZigZagEncoding.putInt(buffer, length);
        // FIXME: we would know at pickler creation time that this is a byte[] or int[] and take the shortcut
        if (byte.class.equals(a.getClass().getComponentType())) {
          buffer.put((byte[]) a);
          size += ((byte[]) a).length;
        } else {
          size += IntStream.range(0, length).map(i -> recursiveWrite(buf, Array.get(a, i))).sum();
        }
        yield size;
      }
      default -> throw new IllegalStateException("Unexpected value: " + object);
    };
  }

  static int maxSizeOf(Object object) {
    // we add 1 for the type marker
    return 1 + switch (object) {
      case Integer i -> Math.min(ZigZagEncoding.sizeOf(i), Integer.BYTES);
      case Long l -> Math.min(ZigZagEncoding.sizeOf(l), Long.BYTES);
      case Short ignored -> Short.BYTES;
      case Byte ignored -> Byte.BYTES;
      case Double ignored -> Double.BYTES;
      case Float ignored -> Float.BYTES;
      case Character ignored -> Character.BYTES;
      case Boolean ignored -> 1;
      case String s -> ZigZagEncoding.sizeOf(s.length()) + s.codePoints()
          .map(cp -> {
            if (cp < 128) {
              return 1;
            } else if (cp < 2048) {
              return 2;
            } else if (cp < 65536) {
              return 3;
            } else {
              return 4;
            }
          }).sum();
      case Enum<?> e -> maxSizeOf(e.getClass().getName()) + maxSizeOf(e.name());
      case Record r -> {
        RecordPickler<?> pickler = (RecordPickler<?>) REGISTRY.get(r.getClass());
        yield Arrays.stream(pickler.componentAccessors)
            .map(a -> {
              try {
                final var value = a.invokeWithArguments(r);
                return value;
              } catch (Throwable e) {
                throw new RuntimeException(e);
              }
            })
            .mapToInt(Companion::maxSizeOf)
            .sum();
      }
      case null -> 0;
      case Optional<?> o when o.isEmpty() -> 1;
      case Optional<?> o -> 1 + maxSizeOf(o.get());
      case Object a when a.getClass().isArray() -> {
        int length = Array.getLength(a);
        if (length == 0) {
          yield 1 + ZigZagEncoding.sizeOf(0);
        }
        // FIXME: the reflection should be done at pickler creation time
        int size = 1 + maxSizeOf(a.getClass().getComponentType().getName()) + ZigZagEncoding.sizeOf(length);
        if (byte.class.equals(a.getClass().getComponentType())) {
          yield size + ((byte[]) a).length;
        } else {
          yield size + IntStream.range(0, length)
              .map(i -> maxSizeOf(Array.get(a, i)))
              .sum();
        }
      }
      default -> throw new AssertionError("not implemented for " + object.getClass().getName());
    };
  }
}
