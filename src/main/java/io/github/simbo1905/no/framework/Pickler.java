package io.github.simbo1905.no.framework;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Constructor;
import java.lang.reflect.RecordComponent;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static io.github.simbo1905.no.framework.Companion.manufactureRecordPickler;
import static io.github.simbo1905.no.framework.PackedBuffer.Constants.*;
import static io.github.simbo1905.no.framework.Pickler.LOGGER;

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
    return Companion.getOrCreate(recordClass, () -> manufactureRecordPickler(recordClass, recordClass.getSimpleName()));
  }

  static <S> Pickler<S> forSealedInterface(Class<S> sealedClass) {
    // Get all permitted record subclasses. This will throw an exception if the class is not sealed or if any of the subclasses are not records or sealed interfaces.
    final Class<?>[] subclasses = Companion.validateSealedRecordHierarchy(sealedClass).toArray(Class<?>[]::new);

    // The following computes the shortest set record names when all the common prefixes are removed when also including the name of the sealed interface itself
    @SuppressWarnings("unchecked") final Map<String, Class<? extends S>> classesByShortName = Stream.concat(Stream.of(sealedClass), Arrays.stream(subclasses))
        .map(cls -> (Class<? extends S>) cls) // Safe due to validateSealedRecordHierarchy
        .collect(Collectors.toMap(
                cls -> cls.getName().substring(
                    Arrays.stream(subclasses)
                        .map(Class::getName)
                        .reduce((a, b) ->
                            !a.isEmpty() && !b.isEmpty() ?
                                a.substring(0,
                                    IntStream.range(0, Math.min(a.length(), b.length()))
                                        .filter(i -> a.charAt(i) != b.charAt(i))
                                        .findFirst()
                                        .orElse(Math.min(a.length(), b.length()))) : "")
                        .orElse("").length()),
                cls -> cls
            )
        );

    @SuppressWarnings("unchecked")
    Map<Class<? extends S>, Pickler<? extends S>> picklersByClass = classesByShortName.entrySet().stream()
        .filter(e -> e.getValue().isRecord())
        .collect(Collectors.toMap(
            Map.Entry::getValue,
            e -> {
              // Double cast required to satisfy compiler
              @SuppressWarnings("unchecked")
              Class<? extends Record> recordCls = (Class<? extends Record>) e.getValue();
              return (Pickler<S>) manufactureRecordPickler(recordCls, e.getKey());
            }
        ));
    return new SealedPickler<>(picklersByClass, classesByShortName);
  }

  class SealedPickler<S> implements Pickler<S> {
    final Map<Class<? extends S>, Pickler<? extends S>> subPicklers;
    final Map<String, Class<? extends S>> classesByShortName;

    public SealedPickler(Map<Class<? extends S>, Pickler<? extends S>> subPicklers, Map<String, Class<? extends S>> classesByShortName) {
      this.subPicklers = subPicklers;
      this.classesByShortName = classesByShortName;
    }

    @Override
    public void serialize(PackedBuffer buf, S object) {
      if (object == null) {
        buf.put(NULL.marker());
        return;
      }
      // Here we simply delegate to the RecordPickler which is configured to first write out it's InternedName
      //noinspection unchecked
      Class<? extends S> concreteType = (Class<? extends S>) object.getClass();
      Pickler<? extends S> pickler = subPicklers.get(concreteType);
      //noinspection unchecked
      ((Pickler<Object>) pickler).serialize(buf, object);
    }

    @Override
    public S deserialize(ByteBuffer buffer) {
      buffer.order(java.nio.ByteOrder.BIG_ENDIAN);
      buffer.mark();
      final byte marker = buffer.get();
      if (marker == NULL.marker()) {
        return null;
      }
      if (marker != INTERNED_NAME.marker() && marker != INTERNED_OFFSET.marker()) {
        throw new IllegalStateException("Expected marker byte for INTERNED_NAME("
            + INTERNED_NAME.marker() + ") or INTERNED_OFFSET("
            + INTERNED_OFFSET.marker() + ") for INTERNED_NAME but got "
            + marker);
      }
      buffer.reset();
      // Read the interned name
      final Pickler.InternedName name = (InternedName) Companion.read(new HashMap<>(), buffer);
      assert name != null;
      final RecordPickler<?> pickler = (RecordPickler<?>) subPicklers.get(classesByShortName.get(name.name()));
      if (pickler == null) {
        throw new IllegalStateException("No pickler found for " + name.name());
      }
      try {
        //noinspection unchecked
        return (S) pickler.deserializeWithMap(new HashMap<>(), buffer);
      } catch (RuntimeException e) {
        throw e;
      } catch (Throwable t) {
        throw new RuntimeException("Failed to deserialize " + name.name() + " : " + t.getMessage(), t);
      }
    }
  }

  final class RecordPickler<R extends Record> implements Pickler<R> {
    final MethodHandle[] componentAccessors;
    final Class<R> recordClass;
    final int componentCount;
    final MethodHandle canonicalConstructorHandle;
    final Map<Integer, MethodHandle> fallbackConstructorHandles = new HashMap<>();
    final Pickler.InternedName internedName;

    RecordPickler(final Class<R> recordClass, Pickler.InternedName internedName) {
      this.internedName = internedName;
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

    void serializeWithMap(PackedBuffer buffer, R object) {
      Object[] components = new Object[componentAccessors.length];
      Arrays.setAll(components, i -> {
        try {
          return componentAccessors[i].invokeWithArguments(object);
        } catch (Throwable e) {
          final var msg = "Failed to access component: " + i +
              " in record class '" + recordClass.getName() + "' : " + e.getMessage();
          LOGGER.severe(() -> msg);
          throw new IllegalArgumentException(msg, e);
        }
      });
      // Write the number of components as an unsigned byte (max 255)
      LOGGER.finer(() -> "serializeWithMap Writing component length length=" + components.length + " position=" + buffer.position());
      ZigZagEncoding.putInt(buffer.buffer, components.length);
      Arrays.stream(components).forEach(c -> {
        buffer.writeComponent(buffer, c);
      });
    }

    @Override
    public void serialize(PackedBuffer buf, R object) {
      // Null checks
      Objects.requireNonNull(buf);
      Objects.requireNonNull(object);
      // Use java native endian for float and double writes
      buf.buffer.order(java.nio.ByteOrder.BIG_ENDIAN);
      // The following `assert` requires jvm flag `-ea` to run. Here we check for a common problem where Java erasure
      // can have you accidentally create arrays of records from collections that are the common supertype of all your records.
      assert 0 == object.getClass().getRecordComponents().length : object.getClass().getName() + " has no components. Built-in collections conversion to arrays may cause this problem.";
      // If we are being asked to write out our record class name by a sealed pickler then we so now
      Optional.ofNullable(internedName).ifPresent(name -> {
        buf.offsetMap.put(internedName, new Pickler.InternedPosition(buf.buffer.position()));
        Companion.write(buf.buffer, internedName);
      });
      // Write the all the components
      serializeWithMap(buf, object);
    }

    @Override
    public R deserialize(ByteBuffer buffer) {
      buffer.order(java.nio.ByteOrder.BIG_ENDIAN);
      try {
        Optional.ofNullable(internedName).ifPresent(name -> {
          final byte marker = buffer.get();
          if (marker != INTERNED_NAME.marker()) {
            throw new IllegalStateException("Expected marker " + INTERNED_NAME.marker() + " but got " + marker);
          }
          // Read the interned name
          final int length = ZigZagEncoding.getInt(buffer);
          byte[] bytes = new byte[length];
          buffer.get(bytes);
          final var internedName = new Pickler.InternedName(new String(bytes, StandardCharsets.UTF_8));
          if (!internedName.equals(this.internedName)) {
            throw new IllegalStateException("Interned name mismatch: expected " + this.internedName + " but got " + internedName);
          }
          LOGGER.finer(() -> "deserializeWithMap reading interned name " + internedName.name() + " position=" + buffer.position());
        });
        return deserializeWithMap(new HashMap<>(), buffer);
      } catch (RuntimeException e) {
        throw e;
      } catch (Throwable t) {
        throw new RuntimeException("Failed to deserialize " + recordClass.getName() + " : " + t.getMessage(), t);
      }
    }

    R deserializeWithMap(Map<Integer, Class<?>> bufferOffset2Class, ByteBuffer buffer) throws Throwable {
      // Read the number of components as an unsigned byte
      LOGGER.finer(() -> "deserializeWithMap reading component length position=" + buffer.position());
      final int length = ZigZagEncoding.getInt(buffer);
      final Object[] components = new Object[length];
      Arrays.setAll(components, i -> {
        try {
          // Read the component
          return Companion.read(bufferOffset2Class, buffer);
        } catch (Throwable e) {
          final var msg = "Failed to access component: " + i +
              " in record class '" + recordClass.getName() + "' : " + e.getMessage();
          LOGGER.severe(() -> msg);
          throw new IllegalArgumentException(msg, e);
        }
      });
      //noinspection unchecked
      return (R) canonicalConstructorHandle.invokeWithArguments(components);
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
  static <R extends Record> Pickler<R> manufactureRecordPickler(final Class<R> recordClass, final String name) {
    return new Pickler.RecordPickler<>(recordClass, name != null ? new Pickler.InternedName(name) : null);
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
    ZigZagEncoding.putInt(buffer, length);
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

  static Object read(Map<Integer, Class<?>> bufferOffset2Class, final ByteBuffer buffer) {
    final byte marker = buffer.get();
    return switch (fromMarker(marker)) {
      case NULL -> null;
      case BOOLEAN -> buffer.get() != 0x0;
      case BYTE -> buffer.get();
      case SHORT -> buffer.getShort();
      case CHARACTER -> buffer.getChar();
      case INTEGER -> buffer.getInt();
      case INTEGER_VAR -> ZigZagEncoding.getInt(buffer);
      case LONG -> buffer.getLong();
      case LONG_VAR -> ZigZagEncoding.getLong(buffer);
      case FLOAT -> buffer.getFloat();
      case DOUBLE -> buffer.getDouble();
      case STRING -> {
        int length = ZigZagEncoding.getInt(buffer);
        byte[] bytes = new byte[length];
        buffer.get(bytes);
        yield new String(bytes, StandardCharsets.UTF_8);
      }
      case OPTIONAL_EMPTY -> Optional.empty();
      case OPTIONAL_OF -> Optional.ofNullable(read(bufferOffset2Class, buffer));
      case INTERNED_NAME, ENUM -> {
        int length = ZigZagEncoding.getInt(buffer);
        byte[] bytes = new byte[length];
        buffer.get(bytes);
        yield new Pickler.InternedName(new String(bytes, StandardCharsets.UTF_8));
      }
      case INTERNED_OFFSET -> {
        final int offset = buffer.getInt();
        final int highWaterMark = buffer.position();
        final int newPosition = buffer.position() + offset - 2;
        buffer.position(newPosition);
        int length = ZigZagEncoding.getInt(buffer);
        byte[] bytes = new byte[length];
        buffer.get(bytes);
        buffer.position(highWaterMark);
        yield new Pickler.InternedName(new String(bytes, StandardCharsets.UTF_8));
      }
      case INTERNED_OFFSET_VAR -> {
        final int highWaterMark = buffer.position();
        final int offset = ZigZagEncoding.getInt(buffer);
        buffer.position(buffer.position() + offset - 1);
        int length = ZigZagEncoding.getInt(buffer);
        byte[] bytes = new byte[length];
        buffer.get(bytes);
        buffer.position(highWaterMark);
        yield new Pickler.InternedName(new String(bytes, StandardCharsets.UTF_8));
      }
      case ARRAY -> throw new AssertionError("not implemented 1");
      case MAP -> throw new AssertionError("not implemented 2");
      case LIST -> throw new AssertionError("not implemented 4");
    };
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

}
