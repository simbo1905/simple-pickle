package io.github.simbo1905.no.framework;

import java.io.*;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Constructor;
import java.lang.reflect.RecordComponent;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

import static io.github.simbo1905.no.framework.Companion.manufactureRecordPickler;
import static io.github.simbo1905.no.framework.PackedBuffer.Constants.*;

public interface Pickler<T> {
  java.util.logging.Logger LOGGER = java.util.logging.Logger.getLogger(Pickler.class.getName());

  /// PackedBuffer is an auto-closeable wrapper around ByteBuffer that tracks the written position of record class names
  /// You should use a try-with-resources block to ensure that it is closed once you have
  /// written a set of records into it. You also cannot use it safely after you have:
  /// - flipped the buffer
  /// - read from the buffer
  default PackedBuffer wrap(ByteBuffer buf) {
    return new PackedBuffer(buf);
  }

  /// PackedBuffer is an auto-closeable wrapper around ByteBuffer that tracks the written position of record class names
  /// You should use a try-with-resources block to ensure that it is closed once you have
  /// written a set of records into it. You also cannot use it safely after you have:
  /// - flipped the buffer
  /// - read from the buffer
  default PackedBuffer allocate(int size) {
    return new PackedBuffer(ByteBuffer.allocate(size));
  }

  /// Recursively loads the components reachable through record into the buffer. It always writes out all the components.
  ///
  /// @param buffer The buffer to write into
  /// @param record The record to serialize
  void serialize(PackedBuffer buffer, T record);

  /// Recursively unloads components from the buffer and invokes a constructor following compatibility rules.
  /// @param buffer The buffer to read from
  /// @return The deserialized record
  T deserialize(ByteBuffer buffer);

  static <R extends Record> Pickler<R> forRecord(Class<R> recordClass) {
    // FIXME change the default here to false
    return Companion.getOrCreate(recordClass, () -> manufactureRecordPickler(recordClass, true));
  }

  static <S> Pickler<S> manufactureSealedPickler(Class<S> sealedClass) {
    return new SealedPickler<>() {
    };
  }

  class SealedPickler<S> implements Pickler<S> {
    @Override
    public S deserialize(ByteBuffer buffer) {
      final var length = buffer.getInt();
      byte[] bytes = new byte[length];
      buffer.get(bytes);
      try (ByteArrayInputStream bin = new ByteArrayInputStream(bytes);
           ObjectInputStream in = new ObjectInputStream(bin)) {
        //noinspection unchecked
        return (S) in.readObject();
      } catch (IOException | ClassNotFoundException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public void serialize(PackedBuffer buf, S animal) {
      //manufactureRecordPickler(animal.getClass()).serialize(buffer, (Record) animal);
    }
  }

  final class RecordPickler<R extends Record> implements Pickler<R> {
    final MethodHandle[] componentAccessors;
    final Class<R> recordClass;
    final int componentCount;
    final MethodHandle canonicalConstructorHandle;
    final Map<Integer, MethodHandle> fallbackConstructorHandles = new HashMap<>();
    private final boolean writeName;

    RecordPickler(final Class<R> recordClass, boolean writeName) {
      this.writeName = writeName;
      this.recordClass = recordClass;
      final RecordComponent[] components = recordClass.getRecordComponents();
      componentCount = components.length;
      final MethodHandles.Lookup lookup = MethodHandles.lookup();
      try {
        // Get parameter types for the canonical constructor
        Class<?>[] parameterTypes = Arrays.stream(components)
            .map(RecordComponent::getType)
            .toArray(Class<?>[]::new);
        // Get the canonical constructor
        Constructor<?> constructorHandle = recordClass.getDeclaredConstructor(parameterTypes);
        canonicalConstructorHandle = lookup.unreflectConstructor(constructorHandle);
        componentAccessors = new MethodHandle[components.length];
        Arrays.setAll(componentAccessors, i -> {
          try {
            return lookup.unreflect(components[i].getAccessor());
          } catch (IllegalAccessException e) {
            final var msg = "Failed to access component accessor for " + components[i].getName() +
                " in record class " + recordClass.getName() + ": " + e.getClass().getSimpleName();
            LOGGER.severe(() -> msg);
            throw new IllegalArgumentException(msg, e);
          }
        });
      } catch (Exception e) {
        Throwable inner = e;
        while (inner.getCause() != null) {
          inner = inner.getCause();
        }
        final var msg = "Failed to access record components for class '" +
            recordClass.getName() + "' due to " + inner.getClass().getSimpleName() + " " + inner.getMessage();
        LOGGER.severe(() -> msg);
        throw new IllegalArgumentException(msg, inner);
      }

      // Get the canonical constructor and any fallback constructors for schema evolution
      try {
        // Get all public constructors
        final Constructor<?>[] allConstructors = recordClass.getConstructors();
        // Filter out the canonical constructor and keep the others as fallbacks
        for (Constructor<?> constructor : allConstructors) {
          int currentParamCount = constructor.getParameterCount();
          MethodHandle handle;
          try {
            handle = lookup.unreflectConstructor(constructor);
          } catch (IllegalAccessException e) {
            LOGGER.warning("Cannot access constructor with " + currentParamCount +
                " parameters for " + recordClass.getName() + ": " + e.getMessage());
            continue;
          }
          // This is a potential fallback constructor for schema evolution
          if (fallbackConstructorHandles.containsKey(currentParamCount)) {
            LOGGER.warning("Multiple fallback constructors with " + currentParamCount +
                " parameters found for " + recordClass.getName() +
                ". Using the first one encountered.");
          } else {
            fallbackConstructorHandles.put(currentParamCount, handle);
            LOGGER.fine("Found fallback constructor with " + currentParamCount +
                " parameters for " + recordClass.getName());
          }
        }
      } catch (Exception e) {
        final var msg = "Failed to access constructors for record '" +
            recordClass.getName() + "' due to " + e.getClass().getSimpleName() + " " + e.getMessage();
        LOGGER.severe(() -> msg);
        throw new IllegalArgumentException(msg, e);
      }
    }

    @Override
    public void serialize(PackedBuffer buf, R object) {
      assert 0 < object.getClass().getRecordComponents().length : object.getClass().getName() + " has no components. Built-in collections conversion to arrays may cause this problem.";
      final var buffer = buf.buffer;
      try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
           ObjectOutputStream out = new ObjectOutputStream(baos)) {
        out.writeObject(object);
        out.flush();
        byte[] bytes = baos.toByteArray();
        buffer.putInt(bytes.length);
        buffer.put(bytes);
      } catch (Exception e) {
        throw new RuntimeException("Failed to serialize record", e);
      }
    }

    @Override
    public R deserialize(ByteBuffer buffer) {
      final var length = buffer.getInt();
      byte[] bytes = new byte[length];
      buffer.get(bytes);
      try (ByteArrayInputStream bin = new ByteArrayInputStream(bytes);
           ObjectInputStream in = new ObjectInputStream(bin)) {
        //noinspection unchecked
        return (R) in.readObject();
      } catch (IOException | ClassNotFoundException e) {
        throw new RuntimeException(e);
      }
    }
  }

  record InternedName(String name) {
  }

  record InternedOffset(int offset) {
  }

  record InternedPosition(int position) {
  }
}

class Companion {
  /// We cache the picklers for each class to avoid creating them multiple times
  static ConcurrentHashMap<Class<?>, Pickler<?>> REGISTRY = new ConcurrentHashMap<>();

  /// Logic to create and cache picklers
  @SuppressWarnings("unchecked")
  static <T> Pickler<T> getOrCreate(Class<T> type, Supplier<Pickler<T>> supplier) {
    return (Pickler<T>) REGISTRY.computeIfAbsent(type, k -> supplier.get());
  }

  ///  Here we are typing things as `Record` to avoid the need for a cast
  static <R extends Record> Pickler<R> manufactureRecordPickler(final Class<R> recordClass, final boolean writeName) {
    return new Pickler.RecordPickler<>(recordClass, writeName);
  }

  static int write(ByteBuffer buffer, int value) {
    if (ZigZagEncoding.sizeOf(value) < Integer.BYTES) {
      buffer.put(PackedBuffer.Constants.INTEGER_VAR.marker());
      return 1 + ZigZagEncoding.putInt(buffer, value);
    } else {
      buffer.put(PackedBuffer.Constants.INTEGER.marker());
      buffer.putInt(value);
      return 1 + Integer.BYTES;
    }
  }

  static int write(ByteBuffer buffer, long value) {
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
    buffer.put(DOUBLE.marker());
    buffer.putDouble(value);
    return 1 + Double.BYTES;
  }

  static int write(ByteBuffer buffer, float value) {
    buffer.put(FLOAT.marker());
    buffer.putFloat(value);
    return 1 + Float.BYTES;
  }

  static int write(ByteBuffer buffer, short value) {
    buffer.put(SHORT.marker());
    buffer.putShort(value);
    return 1 + Short.BYTES;
  }

  static int write(ByteBuffer buffer, char value) {
    buffer.put(CHARACTER.marker());
    buffer.putChar(value);
    return 1 + Character.BYTES;
  }

  static int write(ByteBuffer buffer, boolean value) {
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
    ZigZagEncoding.putInt(buffer, length); // TODO check max string size
    buffer.put(utf8);
    return 1 + length;
  }

  static int writeNull(ByteBuffer buffer) {
    buffer.put(NULL.marker());
    return 1;
  }

  static int write(ByteBuffer buffer, Pickler.InternedName type) {
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

  static <T extends Enum<?>> int write(final Map<Pickler.InternedName, Pickler.InternedPosition> offsetMap, ByteBuffer buffer, String ignoredPrefix, T e) {
    Objects.requireNonNull(e);
    Objects.requireNonNull(ignoredPrefix);
    final var className = e.getDeclaringClass().getName();
    final var shortName = className.substring(ignoredPrefix.length());
    final var dotName = shortName + "." + e.name();
    final var internedName = new Pickler.InternedName(dotName);
    if (!offsetMap.containsKey(internedName)) {
      offsetMap.put(internedName, new Pickler.InternedPosition(buffer.position()));
      buffer.put(ENUM.marker());
      return 1 + intern(buffer, dotName);
    } else {
      final var internedPosition = offsetMap.get(internedName);
      final var internedOffset = new Pickler.InternedOffset(internedPosition.position() - buffer.position());
      return write(buffer, internedOffset);
    }
  }

  static int write(ByteBuffer buffer, Pickler.InternedOffset typeOffset) {
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
        case Pickler.InternedName t -> write(buffer, t);
        case Pickler.InternedOffset t -> write(buffer, t);
        default -> throw new AssertionError("unknown optional value " + value);
      };
      return 1 + innerSize;
    } else {
      buffer.put(OPTIONAL_EMPTY.marker());
      return 1;
    }
  }
}
