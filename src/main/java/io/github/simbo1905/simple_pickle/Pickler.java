package io.github.simbo1905.simple_pickle;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.reflect.Array;
import java.lang.reflect.Constructor;
import java.lang.reflect.RecordComponent;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static io.github.simbo1905.simple_pickle.Companion.*;
import static io.github.simbo1905.simple_pickle.Constants.*;
import static io.github.simbo1905.simple_pickle.Pickler.LOGGER;
import static java.nio.charset.StandardCharsets.UTF_8;

public interface Pickler<T> {
  java.util.logging.Logger LOGGER = java.util.logging.Logger.getLogger(Pickler.class.getName());

  Map<Class<?>, Pickler<?>> REGISTRY = new ConcurrentHashMap<>();

  // In Pickler interface
  @SuppressWarnings("unchecked")
  static <T> Pickler<T> getOrCreate(Class<T> type, Supplier<Pickler<T>> supplier) {
    return (Pickler<T>) REGISTRY.computeIfAbsent(type, k -> supplier.get());
  }

  void serialize(T object, ByteBuffer buffer);

  T deserialize(ByteBuffer buffer);

  int sizeOf(T value);

  static <R extends Record> Pickler<R> forRecord(Class<R> recordClass) {
    return RecordPickler.create(recordClass);
  }

  static <S> Pickler<S> forSealedInterface(Class<S> sealedClass) {
    return SealedPickler.create(sealedClass);
  }

  static <R extends Record> void serializeMany(R[] array, ByteBuffer buffer) {
    buffer.put(typeMarker(ARRAY));
    byte[] classNameBytes = array.getClass().getComponentType().getName().getBytes(UTF_8);
    buffer.putInt(classNameBytes.length);
    buffer.put(classNameBytes);
    buffer.putInt(array.length);

    @SuppressWarnings("unchecked") Pickler<R> pickler = Pickler.forRecord((Class<R>) array.getClass().getComponentType());
    Arrays.stream(array).forEach(element -> pickler.serialize(element, buffer));
  }

  static <R extends Record> List<R> deserializeMany(Class<R> componentType, ByteBuffer buffer) {
    byte marker = buffer.get();
    if (marker != typeMarker(ARRAY)) throw new IllegalArgumentException("Invalid array marker");

    Class<?> readType;
    int classNameLength = buffer.getInt();
    byte[] classNameBytes = new byte[classNameLength];
    buffer.get(classNameBytes);
    String className = new String(classNameBytes, UTF_8);

    try {
      readType = Class.forName(className);
    } catch (ClassNotFoundException e) {
      throw new IllegalArgumentException("Unknown subtype: " + className);
    }
    if (!componentType.equals(readType)) throw new IllegalArgumentException("Type mismatch");

    return IntStream.range(0, buffer.getInt())
        .mapToObj(i -> Pickler.forRecord(componentType).deserialize(buffer))
        .toList();
  }

  static <R extends Record> int sizeOfMany(R[] array) {
    //noinspection unchecked
    return Optional.ofNullable(array)
        .map(arr -> 1 + 4 // 4 bytes for the length prefix (int)
            + arr.getClass().getComponentType().getName().getBytes(UTF_8).length + 4 +
            Arrays.stream(arr)
                .mapToInt(Pickler.forRecord((Class<R>) arr.getClass().getComponentType())::sizeOf)
                .sum())
        .orElse(1);
  }

  static <S> void serializeManySealed(List<? extends S> list, ByteBuffer buffer) {
    buffer.put(typeMarker(LIST));
    buffer.putInt(list.size());

    list.forEach(element -> {
      byte[] classNameBytes = element.getClass().getComponentType().getName().getBytes(UTF_8);
      buffer.putInt(classNameBytes.length);
      buffer.put(classNameBytes);
      @SuppressWarnings("unchecked")
      Pickler<S> pickler = (Pickler<S>) forSealedInterface(element.getClass());
      pickler.serialize(element, buffer);
    });
  }

  static <S> List<S> deserializeManySealed(Class<S> sealedInterface, ByteBuffer buffer) {
    byte marker = buffer.get();
    if (marker != typeMarker(LIST)) throw new IllegalArgumentException("Invalid list marker");

    return IntStream.range(0, buffer.getInt())
        .mapToObj(i -> {
          // Read class name from buffer
          int classNameLength = buffer.getInt();
          byte[] classNameBytes = new byte[classNameLength];
          buffer.get(classNameBytes);
          String className = new String(classNameBytes, UTF_8);

          // Load class and validate
          Class<? extends S> concreteType;
          try {
            //noinspection unchecked
            concreteType = (Class<? extends S>) Class.forName(className);
          } catch (ClassNotFoundException e) {
            throw new IllegalArgumentException("Unknown subtype: " + className);
          }
          if (!sealedInterface.isAssignableFrom(concreteType)) {
            throw new IllegalArgumentException("Invalid type: " + className);
          }

          // Deserialize using concrete pickler
          return (S) Pickler.forSealedInterface(concreteType).deserialize(buffer);
        })
        .toList();
  }

  static <S> int sizeOfManySealed(List<? extends S> list) {
    //noinspection unchecked
    return Optional.ofNullable(list)
        .map(l -> 1 + 4 + l.stream()
            .mapToInt(element ->
                4 // 4 bytes for the length prefix (int)
                    + element.getClass().getName().getBytes(UTF_8).length +
                    ((Pickler<S>) forSealedInterface(element.getClass())).sizeOf(element)
            )
            .sum())
        .orElse(1);
  }
}

abstract class SealedPickler<S> implements Pickler<S> {
  /// Helper method to recursively find all permitted record classes
  static Stream<Class<?>> allPermittedRecordClasses(Class<?> sealedClass) {
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
            return allPermittedRecordClasses(subclass);
          } else {
            final var msg = "Permitted subclass must be either a record or sealed interface: " +
                subclass.getName();
            LOGGER.severe(() -> msg);
            throw new IllegalArgumentException(msg);
          }
        });
  }

  @Override
  public S deserialize(ByteBuffer buffer) {
    // Stub implementation
    return null;
  }

  @Override
  public void serialize(S object, ByteBuffer buffer) {
    // Stub
  }

  @Override
  public int sizeOf(S value) {
    return 0; // Stub
  }

  static <S> Pickler<S> create(Class<S> sealedClass) {
    return Pickler.getOrCreate(sealedClass, () -> manufactureSealedPickler(sealedClass));
  }

  private static <S> Pickler<S> manufactureSealedPickler(Class<S> sealedClass) {
    // Get all permitted record subclasses
    Class<?>[] subclasses = allPermittedRecordClasses(sealedClass).toArray(Class<?>[]::new);

    @SuppressWarnings("unchecked") Map<Class<? extends S>, Pickler<? extends S>> subPicklers = Arrays.stream(subclasses)
        .filter(cls -> cls.isRecord() || cls.isSealed())
        .map(cls -> (Class<? extends S>) cls) // Safe due to sealed hierarchy
        .collect(Collectors.toMap(
            cls -> cls,
            cls -> {
              if (cls.isRecord()) {
                // Double cast required to satisfy compiler
                @SuppressWarnings("unchecked")
                Class<? extends Record> recordCls = (Class<? extends Record>) cls;
                @SuppressWarnings("unchecked")
                Pickler<S> pickler = (Pickler<S>) Pickler.forRecord(recordCls);
                return pickler;
              } else {
                return Pickler.forSealedInterface(cls);
              }
            }
        ));

    return new SealedPickler<>() {
      @Override
      public void serialize(S object, ByteBuffer buffer) {
        if (object == null) {
          buffer.put(NULL.marker());
          return;
        }
        @SuppressWarnings("unchecked") Class<? extends S> concreteType = (Class<? extends S>) object.getClass();
        Pickler<? extends S> pickler = subPicklers.get(concreteType);

        // Write type identifier
        writeClassName(buffer, concreteType);

        // Delegate to subtype pickler
        //noinspection unchecked
        ((Pickler<Object>) pickler).serialize(object, buffer);
      }

      @Override
      public S deserialize(ByteBuffer buffer) {
        // if the type is NULL, return null, else read the type identifier
        buffer.mark();
        if (buffer.get() == NULL.marker()) {
          return null;
        }
        buffer.reset();
        // Read type identifier
        Class<? extends S> concreteType = readClass(buffer);

        // Get subtype pickler
        Pickler<? extends S> pickler = subPicklers.get(concreteType);

        return pickler.deserialize(buffer);
      }

      @Override
      public int sizeOf(S object) {
        if (object == null) {
          return 1; // Size of NULL marker
        }
        // TODO: size of the class name of the type then resolve the pickler and ask it to calculate the size and add
        return 0;
      }

      private void writeClassName(ByteBuffer buffer, Class<?> type) {
        byte[] classNameBytes = type.getName().getBytes(UTF_8);
        buffer.putInt(classNameBytes.length);
        buffer.put(classNameBytes);
      }

      @SuppressWarnings("unchecked")
      private Class<? extends S> readClass(ByteBuffer buffer) {
        int classNameLength = buffer.getInt();
        byte[] classNameBytes = new byte[classNameLength];
        buffer.get(classNameBytes);
        String className = new String(classNameBytes, UTF_8);

        try {
          return (Class<? extends S>) Class.forName(className);
        } catch (ClassNotFoundException e) {
          throw new IllegalArgumentException("Unknown subtype: " + className);
        }
      }
    };
  }
}

abstract class RecordPickler<R extends Record> implements Pickler<R> {

  static <R extends Record> Pickler<R> create(Class<R> recordClass) {
    return Pickler.getOrCreate(recordClass, () -> manufactureRecordPickler(recordClass));
  }

  private static <R extends Record> Pickler<R> manufactureRecordPickler(Class<R> recordClass) {
    final Map<Integer, MethodHandle> fallbackConstructorHandles = new HashMap<>();
    final MethodHandle[] componentAccessors;
    final int canonicalParamCount;
    final MethodHandles.Lookup lookup = MethodHandles.lookup();
    MethodHandle canonicalConstructorHandle;

    // Get lookup object

    // First we get the accessor method handles for the record components and add them to the array used
    // that is used by the base class to pull all the components out of the record to load into the byte buffer
    try {
      RecordComponent[] components = recordClass.getRecordComponents();
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
      // Get the record components
      final RecordComponent[] components = recordClass.getRecordComponents();
      // Extract component types for the canonical constructor
      final Class<?>[] canonicalParamTypes = Arrays.stream(components)
          .map(RecordComponent::getType)
          .toArray(Class<?>[]::new);
      canonicalParamCount = canonicalParamTypes.length;

      // Get all public constructors
      final Constructor<?>[] allConstructors = recordClass.getConstructors();

      // Find the canonical constructor and potential fallback constructors
      canonicalConstructorHandle = null;

      for (Constructor<?> constructor : allConstructors) {
        Class<?>[] currentParamTypes = constructor.getParameterTypes();
        int currentParamCount = constructor.getParameterCount();
        MethodHandle handle;

        try {
          handle = lookup.unreflectConstructor(constructor);
        } catch (IllegalAccessException e) {
          LOGGER.warning("Cannot access constructor with " + currentParamCount +
              " parameters for " + recordClass.getName() + ": " + e.getMessage());
          continue;
        }

        if (Arrays.equals(currentParamTypes, canonicalParamTypes)) {
          // Found the canonical constructor
          canonicalConstructorHandle = handle;
        } else {
          // This is a potential fallback constructor for schema evolution
          if (fallbackConstructorHandles.containsKey(currentParamCount)) {
            LOGGER.warning("Multiple fallback constructors with " + currentParamCount +
                " parameters found for " + recordClass.getName() +
                ". Using the first one encountered.");
            // We keep the first one we found
          } else {
            fallbackConstructorHandles.put(currentParamCount, handle);
            LOGGER.fine("Found fallback constructor with " + currentParamCount +
                " parameters for " + recordClass.getName());
          }
        }
      }

      // If we didn't find the canonical constructor, try to find it directly
      if (canonicalConstructorHandle == null) {
        try {
          // Create method type for the canonical constructor
          MethodType constructorType = MethodType.methodType(void.class, canonicalParamTypes);
          canonicalConstructorHandle = lookup.findConstructor(recordClass, constructorType);
        } catch (NoSuchMethodException | IllegalAccessException e) {
          final var msg = "Failed to access canonical constructor for record '" +
              recordClass.getName() + "' due to " + e.getClass().getSimpleName();
          LOGGER.severe(() -> msg);
          throw new IllegalArgumentException(msg, e);
        }
      }
    } catch (Exception e) {
      final var msg = "Failed to access constructors for record '" +
          recordClass.getName() + "' due to " + e.getClass().getSimpleName() + " " + e.getMessage();
      LOGGER.severe(() -> msg);
      throw new IllegalArgumentException(msg, e);
    }

    // Capture these values for use in the anonymous class
    final MethodHandle finalCanonicalConstructorHandle = canonicalConstructorHandle;
    final String recordClassName = recordClass.getName();
    final Map<Integer, MethodHandle> finalFallbackConstructorHandles =
        Collections.unmodifiableMap(fallbackConstructorHandles);

    return new RecordPickler<>() {
      private Object[] components(R record) {
        Object[] result = new Object[componentAccessors.length];
        Arrays.setAll(result, i -> {
          try {
            return componentAccessors[i].invokeWithArguments(record);
          } catch (Throwable e) {
            final var msg = "Failed to access component: " + i +
                " in record class '" + recordClassName + "' : " + e.getMessage();
            LOGGER.severe(() -> msg);
            throw new IllegalArgumentException(msg, e);
          }
        });
        return result;
      }

      @SuppressWarnings("unchecked")
      private R staticCreateFromComponents(Object[] components) {
        try {
          // Get the number of components from the serialized data
          int numComponents = components.length;
          MethodHandle constructorToUse;

          if (numComponents == canonicalParamCount) {
            // Number of components matches the canonical constructor - use it directly
            constructorToUse = finalCanonicalConstructorHandle;
            LOGGER.finest(() -> "Using canonical constructor for " + recordClassName +
                " with " + numComponents + " components");
          } else {
            // Number of components differs, look for a fallback constructor
            constructorToUse = finalFallbackConstructorHandles.get(numComponents);
            if (constructorToUse == null) {
              final var msg = "Schema evolution error: Cannot deserialize data for " +
                  recordClassName + ". Found " + numComponents +
                  " components, but no matching constructor (canonical or fallback) exists.";
              LOGGER.severe(() -> msg);
              // No fallback constructor matches the number of components found
              throw new IllegalArgumentException(msg);
            }
            LOGGER.finest(() -> "Using fallback constructor for " + recordClassName +
                " with " + numComponents + " components");
          }

          // Invoke the selected constructor
          return (R) constructorToUse.invokeWithArguments(components);
        } catch (Throwable e) {
          final var msg = "Failed to create instance of " + recordClassName +
              " with " + components.length + " components: " + e.getMessage();
          LOGGER.severe(() -> msg);
          throw new IllegalArgumentException(msg, e);
        }
      }

      @Override
      public void serialize(R object, ByteBuffer buffer) {
        final var components = components(object);
        // Write the number of components as an unsigned byte (max 255)
        writeUnsignedByte(buffer, (short) components.length);
        Arrays.stream(components).forEach(c -> Companion.write(new HashMap<>(), buffer, c));
      }

      @Override
      public R deserialize(ByteBuffer buffer) {
        // Read the number of components as an unsigned byte
        final short length = readUnsignedByte(buffer);
        final var components = new Object[length];
        Arrays.setAll(components, ignored -> deserializeValue(new HashMap<>(), buffer));
        return this.staticCreateFromComponents(components);
      }

      @Override
      public int sizeOf(R object) {
        final var components = components(object);
        int size = 1; // Start with 1 byte for the type of the component
        for (Object c : components) {
          size += staticSizeOf(c, new HashSet<>());
          int finalSize = size;
          LOGGER.finer(() -> "Size of " +
              Optional.ofNullable(c).map(c2 -> c2.getClass().getSimpleName()).orElse("null")
              + " '" + c + "' is " + finalSize);
        }
        return size;
      }
    };
  }
}

/// Enum containing constants used throughout the Pickler implementation
enum Constants {
  NULL((byte) 1, 0, null),
  BOOLEAN((byte) 2, 1, boolean.class),
  BYTE((byte) 3, Byte.BYTES, byte.class),
  SHORT((byte) 4, Short.BYTES, short.class),
  CHARACTER((byte) 5, Character.BYTES, char.class),
  INTEGER((byte) 6, Integer.BYTES, int.class),
  LONG((byte) 7, Long.BYTES, long.class),
  FLOAT((byte) 8, Float.BYTES, float.class),
  DOUBLE((byte) 9, Double.BYTES, double.class),
  STRING((byte) 10, 0, String.class),
  OPTIONAL((byte) 11, 0, Optional.class),
  RECORD((byte) 12, 0, Record.class),
  ARRAY((byte) 13, 0, null),
  MAP((byte) 14, 0, Map.class),
  ENUM((byte) 15, 0, Enum.class),
  LIST((byte) 16, 0, List.class);

  private final byte typeMarker;
  private final int sizeInBytes;
  private final Class<?> clazz;

  Constants(byte typeMarker, int sizeInBytes, Class<?> clazz) {
    this.typeMarker = typeMarker;
    this.sizeInBytes = sizeInBytes;
    this.clazz = clazz;
  }

  public byte marker() {
    return typeMarker;
  }

  public int getSizeInBytes() {
    return sizeInBytes;
  }

  public Class<?> _class() {
    return clazz;
  }

  public static Constants fromMarker(byte marker) {
    for (Constants c : values()) {
      if (c.typeMarker == marker) {
        return c;
      }
    }
    final var msg = "Unknown type marker: " + marker;
    LOGGER.severe(() -> msg);
    throw new IllegalArgumentException(msg);
  }
}

class Companion {

  /// Writes a short value (0-255) as a single unsigned byte to the buffer.
  ///
  /// @param buffer The buffer to write to.
  /// @param value The short value (must be between 0 and 255).
  /// @throws IllegalArgumentException if the value is outside the 0-255 range.
  static void writeUnsignedByte(ByteBuffer buffer, short value) {
    if (value < 0 || value > 255) {
      throw new IllegalArgumentException("Value must be between 0 and 255, but got: " + value);
    }
    buffer.put((byte) value);
  }

  /// Reads a single byte from the buffer and returns it as a short, treating the byte as unsigned (0-255).
  ///
  /// @param buffer The buffer to read from.
  /// @return The short value (0-255).
  static short readUnsignedByte(ByteBuffer buffer) {
    return (short) (buffer.get() & 0xFF);
  }

  static void write(Map<Class<?>, Integer> classToOffset, ByteBuffer buffer, Object c) {
    int startPosition = buffer.position();
    LOGGER.finest(() -> "Starting write at position " + startPosition +
        " for " + (c != null ? c.getClass().getName() : "null") +
        ", remaining: " + buffer.remaining());

    if (c == null) {
      buffer.put(NULL.marker());
      LOGGER.finest(() -> "Wrote NULL marker, new position: " + buffer.position());
      return;
    }

    if (c.getClass().isArray()) {
      buffer.put(ARRAY.marker());

      writeDeduplicatedClassName(buffer, c.getClass().getComponentType(), classToOffset);

      // Write the array length
      int length = Array.getLength(c);
      buffer.putInt(length);

      if (byte.class.equals(c.getClass().getComponentType())) {
        buffer.put((byte[]) c);
      } else {
        IntStream.range(0, length).forEach(i -> write(classToOffset, buffer, Array.get(c, i)));
      }

      return;
    }

    switch (c) {
      case Integer i -> buffer.put(typeMarker(c)).putInt(i);
      case Long l -> buffer.put(typeMarker(c)).putLong(l);
      case Short s -> buffer.put(typeMarker(c)).putShort(s);
      case Byte b -> buffer.put(typeMarker(c)).put(b);
      case Double d -> buffer.put(typeMarker(c)).putDouble(d);
      case Float f -> buffer.put(typeMarker(c)).putFloat(f);
      case Character ch -> buffer.put(typeMarker(c)).putChar(ch);
      case Boolean bool -> buffer.put(typeMarker(c)).put((byte) (bool ? 1 : 0));
      case String str -> {
        buffer.put(typeMarker(c));
        final var bytes = str.getBytes(UTF_8);
        buffer.putShort((short) bytes.length); // Using full int instead of byte
        buffer.put(bytes);
      }
      case Optional<?> opt -> {
        buffer.put(typeMarker(c));
        if (opt.isEmpty()) {
          buffer.put((byte) 0); // 0 = empty
        } else {
          buffer.put((byte) 1); // 1 = present
          Object value = opt.get();
          write(classToOffset, buffer, value);
        }
      }
      case Record record -> {
        buffer.put(typeMarker(c));

        // Write the class name with deduplication
        writeDeduplicatedClassName(buffer, record.getClass(), classToOffset);

        // Get the appropriate pickler for this record type
        @SuppressWarnings("unchecked")
        Pickler<Record> nestedPickler = (Pickler<Record>) Pickler.forRecord(record.getClass());

        nestedPickler.serialize(record, buffer);
      }
      case Map<?, ?> map -> {
        buffer.put(typeMarker(c));

        // Write the number of entries
        buffer.putInt(map.size());

        // Write each key-value pair
        map.forEach((key, value) -> {
          // Write the key
          write(classToOffset, buffer, key);
          // Write the value
          write(classToOffset, buffer, value);
        });
      }
      case List<?> list -> {
        buffer.put(typeMarker(c));

        // Write the number of elements
        buffer.putInt(list.size());

        // Write each element
        list.forEach(element -> write(classToOffset, buffer, element));
      }
      case Enum<?> enumValue -> {
        int enumStartPos = buffer.position();
        LOGGER.finest(() -> "Starting enum serialization at position " + enumStartPos);

        buffer.put(typeMarker(c));
        LOGGER.finest(() -> "Wrote enum type marker: " + ENUM.marker());

        // Write the enum class name with deduplication
        int beforeClassName = buffer.position();
        writeDeduplicatedClassName(buffer, enumValue.getClass(), classToOffset);
        int afterClassName = buffer.position();
        LOGGER.finest(() -> "Wrote enum class name from position " + beforeClassName +
            " to " + afterClassName + " (size: " + (afterClassName - beforeClassName) + ")");

        // Write the enum constant name
        String enumConstantName = enumValue.name();
        byte[] enumNameBytes = enumConstantName.getBytes(UTF_8);
        LOGGER.finest(() -> "Writing enum constant name: " + enumConstantName +
            ", length: " + enumNameBytes.length);

        int beforeConstantName = buffer.position();
        buffer.putInt(enumNameBytes.length);
        buffer.put(enumNameBytes);
        int afterConstantName = buffer.position();
        LOGGER.finest(() -> "Wrote enum constant name from position " + beforeConstantName +
            " to " + afterConstantName + " (size: " + (afterConstantName - beforeConstantName) + ")");

        LOGGER.finest(() -> "Completed enum serialization, total size: " +
            (buffer.position() - enumStartPos));
      }
      default -> throw new IllegalArgumentException("Unsupported type: " + c.getClass());
    }
  }

  /// Helper method to write a class name to a buffer with deduplication.
  /// If the class has been seen before, writes a negative reference instead of the full name.
  ///
  /// @param buffer The buffer to write to
  /// @param clazz The class to write
  /// @param classToOffset Map tracking class to buffer position offset
  public static void writeDeduplicatedClassName(ByteBuffer buffer, Class<?> clazz,
                                               Map<Class<?>, Integer> classToOffset) {
    LOGGER.finest(() -> "writeDeduplicatedClassName: class=" + clazz.getName() +
        ", buffer position=" + buffer.position() +
        ", map contains class=" + classToOffset.containsKey(clazz));

    // Check if we've seen this class before
    Integer offset = classToOffset.get(clazz);
    if (offset != null) {
      // We've seen this class before, write a negative reference
      int reference = ~offset;
      buffer.putInt(reference); // Using bitwise complement for negative reference
      LOGGER.finest(() -> "Wrote class reference: " + clazz.getName() +
          " at offset " + offset + ", value=" + reference);
    } else {
      // First time seeing this class, write the full name
      String className = clazz.getName();
      byte[] classNameBytes = className.getBytes(UTF_8);
      int classNameLength = classNameBytes.length;

      // Store current position before writing
      int currentPosition = buffer.position();

      // Write positive length and class name
      buffer.putInt(classNameLength);
      buffer.put(classNameBytes);

      // Store the position where we wrote this class
      classToOffset.put(clazz, currentPosition);
      LOGGER.finest(() -> "Wrote class name: " + className +
          " at offset " + currentPosition +
          ", length=" + classNameLength +
          ", new buffer position=" + buffer.position());
    }
  }

  static byte typeMarker(Object c) {
    if (c == null) {
      return NULL.marker();
    }
    if (c.getClass().isArray()) {
      return ARRAY.marker();
    }
    if (c instanceof Enum<?>) {
      return ENUM.marker();
    }
    return switch (c) {
      case Integer ignored -> INTEGER.marker();
      case Long ignored -> LONG.marker();
      case Short ignored -> SHORT.marker();
      case Byte ignored -> BYTE.marker();
      case Double ignored -> DOUBLE.marker();
      case Float ignored -> FLOAT.marker();
      case Character ignored -> CHARACTER.marker();
      case Boolean ignored -> BOOLEAN.marker();
      case String ignored -> STRING.marker();
      case Optional<?> ignored -> OPTIONAL.marker();
      case Record ignored -> RECORD.marker();
      case Map<?, ?> ignored -> MAP.marker();
      case List<?> ignored -> LIST.marker();
      default -> throw new IllegalArgumentException("Unsupported type: " + c.getClass());
    };
  }

  static Object deserializeValue(Map<Integer, Class<?>> bufferOffset2Class, ByteBuffer buffer) {
    final byte type = buffer.get();
    final Constants typeEnum = fromMarker(type);
    return switch (typeEnum) {
      case INTEGER -> buffer.getInt();
      case LONG -> buffer.getLong();
      case SHORT -> buffer.getShort();
      case BYTE -> buffer.get();
      case DOUBLE -> buffer.getDouble();
      case FLOAT -> buffer.getFloat();
      case CHARACTER -> buffer.getChar();
      case BOOLEAN -> buffer.get() == 1;
      case STRING -> {
        final var strLength = buffer.getShort();
        final byte[] bytes = new byte[strLength];
        buffer.get(bytes);
        yield new String(bytes, UTF_8);
      }
      case OPTIONAL -> {
        byte isPresent = buffer.get();
        if (isPresent == 0) {
          yield Optional.empty();
        } else {
          Object value = deserializeValue(bufferOffset2Class, buffer);
          yield Optional.ofNullable(value);
        }
      }
      case RECORD -> { // Handle nested record
        try {
          // Read the class with deduplication support
          Class<?> recordClass = resolveClass(buffer, bufferOffset2Class);

          // Get or create the pickler for this class
          @SuppressWarnings("unchecked")
          Pickler<Record> nestedPickler = (Pickler<Record>) Pickler.forRecord((Class<? extends Record>) recordClass);

          // Deserialize the nested record
          yield nestedPickler.deserialize(buffer);
        } catch (ClassNotFoundException e) {
          final var msg = "Failed to load class: " + e.getMessage();
          LOGGER.severe(() -> msg);
          throw new IllegalArgumentException(msg, e);
        }
      }
      case NULL -> null; // Handle null values
      case ARRAY -> { // Handle arrays
        try {
          // Get the component class
          Class<?> componentType = resolveClass(buffer, bufferOffset2Class);

          // Read array length
          int length = buffer.getInt();

          // Create array of the right type and size
          final Object array = Array.newInstance(componentType, length);

          if (componentType.equals(byte.class)) {
            buffer.get((byte[]) array);
          } else {
            // Deserialize each element using IntStream instead of for loop
            IntStream.range(0, length)
                .forEach(i -> Array.set(array, i, deserializeValue(bufferOffset2Class, buffer)));
          }

          yield array;
        } catch (ClassNotFoundException e) {
          final var msg = "Failed to load component class: " + e.getMessage();
          LOGGER.severe(() -> msg);
          throw new IllegalArgumentException(msg, e);
        }
      }
      case MAP -> // Handle maps
          IntStream.range(0, buffer.getInt())
              .mapToObj(i ->
                  Map.entry(
                      Objects.requireNonNull(deserializeValue(bufferOffset2Class, buffer)),
                      Objects.requireNonNull(deserializeValue(bufferOffset2Class, buffer))))
              .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));
      case LIST -> // Handle Lists
          IntStream.range(0, buffer.getInt())
              .mapToObj(i -> deserializeValue(bufferOffset2Class, buffer))
              .toList();
      case ENUM -> { // Handle enums
        try {
          // Read the enum class with deduplication support
          Class<?> enumClass = resolveClass(buffer, bufferOffset2Class);

          // Verify it's an enum class
          if (!enumClass.isEnum()) {
            final var msg = "Expected enum class but got: " + enumClass.getName();
            LOGGER.severe(() -> msg);
            throw new IllegalArgumentException(msg);
          }

          // Read the enum constant name
          int enumNameLength = buffer.getInt();
          byte[] enumNameBytes = new byte[enumNameLength];
          buffer.get(enumNameBytes);
          String enumName = new String(enumNameBytes, UTF_8);

          // Get the enum constant using helper method with proper type witness
          yield enumValueOf(enumClass, enumName);
        } catch (ClassNotFoundException e) {
          final var msg = "Failed to load enum class: " + e.getMessage();
          LOGGER.severe(() -> msg);
          throw new IllegalArgumentException(msg, e);
        }
      }
    };
  }

  /// Helper method to read a class name from a buffer with deduplication support.
  ///
  /// @param buffer The buffer to read from
  /// @param bufferOffset2Class Map tracking buffer position to class
  /// @return The loaded class
  public static Class<?> resolveClass(ByteBuffer buffer,
                                      Map<Integer, Class<?>> bufferOffset2Class)
      throws ClassNotFoundException {

    int bufferPosition = buffer.position();

    LOGGER.finest(() -> "resolveClass: buffer position=" + bufferPosition +
        ", remaining=" + buffer.remaining());

    // Read the class name length or reference
    int componentTypeLength = buffer.getInt();

    if (componentTypeLength > Short.MAX_VALUE) {
      final var msg = "The max length of a string in java is 65535 bytes, " +
          "but the length of the class name is " + componentTypeLength;
      LOGGER.severe(() -> msg);
      throw new IllegalArgumentException(msg);
    }

    LOGGER.finest(() -> "Read length/reference value: " + componentTypeLength +
        ", buffer position after read=" + buffer.position());

    if (componentTypeLength < 0) {
      // This is a reference to a previously seen class
      int offset = ~componentTypeLength; // Decode the reference using bitwise complement
      Class<?> referencedClass = bufferOffset2Class.get(offset);

      LOGGER.finest(() -> "Reference detected. Offset=" + offset +
          ", map contains offset=" + bufferOffset2Class.containsKey(offset) +
          ", referenced class=" + (referencedClass != null ? referencedClass.getName() : "null"));

      if (referencedClass == null) {
        final var msg = "Invalid class reference offset: " + offset;
        LOGGER.severe(() -> msg);
        throw new IllegalArgumentException(msg);
      }
      return referencedClass;
    } else {
      // This is a new class name
      int currentPosition = buffer.position() - 4; // Position before reading the length

      LOGGER.finest(() -> "New class name detected. Length=" + componentTypeLength +
          ", stored position=" + currentPosition +
          ", remaining bytes=" + buffer.remaining());

      if (buffer.remaining() < componentTypeLength) {
        final var msg = "Buffer underflow: needed " + componentTypeLength +
            " bytes but only " + buffer.remaining() + " remaining";
        LOGGER.severe(() -> msg);
        throw new IllegalArgumentException(msg);
      }

      // Read the class name
      byte[] classNameBytes = new byte[componentTypeLength];
      buffer.get(classNameBytes);
      String className = new String(classNameBytes, UTF_8);

      // Validate class name - add basic validation that allows array type names like `[I`, `[[I`, `[L`java.lang.String;` etc.
      if (!className.matches("[\\[\\]a-zA-Z0-9_.$;]+")) {
        final var msg = "Invalid class name format: " + className;
        LOGGER.severe(() -> msg);
        throw new IllegalArgumentException(msg);
      }

      // Load the class using our helper method
      Class<?> loadedClass = getClassForName(className);

      // Store in our map for future references
      bufferOffset2Class.put(currentPosition, loadedClass);
      LOGGER.finest(() -> "Read class name at offset " + currentPosition +
          ": " + className +
          ", new buffer position=" + buffer.position());

      return loadedClass;
    }
  }

  /// Helper method to get an enum constant with proper type witness
  ///
  /// @param enumClass The enum class
  /// @param enumName The name of the enum constant
  /// @return The enum constant
  @SuppressWarnings("unchecked")
  public static <E extends Enum<E>> Object enumValueOf(Class<?> enumClass, String enumName) {
    return Enum.valueOf((Class<E>) enumClass, enumName);
  }

  /// Class.forName cannot handle primitive types directly, so we need to map them to their wrapper classes.
  static Class<?> getClassForName(String name) throws ClassNotFoundException {
    // Handle primitive types which can't be loaded directly with Class.forName
    return switch (name) {
      case "boolean" -> BOOLEAN._class();
      case "byte" -> BYTE._class();
      case "char" -> CHARACTER._class();
      case "short" -> SHORT._class();
      case "int" -> INTEGER._class();
      case "long" -> LONG._class();
      case "float" -> FLOAT._class();
      case "double" -> DOUBLE._class();
      default -> Class.forName(name);
    };
  }

  static int staticSizeOf(Object c, Set<Class<?>> classes) {
    if (c == null) {
      LOGGER.finest(() -> "Size of null is 1 byte");
      return 1;
    }
    int plainSize = switch (c) {
      case Integer ignored -> INTEGER.getSizeInBytes();
      case Long ignored -> LONG.getSizeInBytes();
      case Short ignored -> SHORT.getSizeInBytes();
      case Byte ignored -> BYTE.getSizeInBytes();
      case Double ignored -> DOUBLE.getSizeInBytes();
      case Float ignored -> FLOAT.getSizeInBytes();
      case Character ignored -> CHARACTER.getSizeInBytes();
      case Boolean ignored -> BOOLEAN.getSizeInBytes();
      default -> 0;
    };
    int size = 1; // Type marker byte
    final int initialSize = size; // Make a final copy for the lambda
    LOGGER.finest(() -> "Starting size calculation for " + c.getClass().getName() +
        ", initial size (type marker): " + initialSize);
    if (c.getClass().isArray()) {
      final int[] arrayHeaderSize = {4 + 4}; // 4 bytes for length + 4 bytes for type name length - use array for mutability
      LOGGER.finest(() -> "Array header base size: " + arrayHeaderSize[0]);

      if (!classes.contains(c.getClass())) {
        // Component type name size
        String componentTypeName = c.getClass().getComponentType().getName();
        byte[] componentTypeBytes = componentTypeName.getBytes(UTF_8);
        arrayHeaderSize[0] += componentTypeBytes.length;
        LOGGER.finest(() -> "Adding component type name size: " + componentTypeBytes.length +
            " for " + componentTypeName);
        classes.add(c.getClass());
      } else {
        LOGGER.finest(() -> "Component type already seen, using reference");
      }

      // Calculate size of all array elements
      int length = Array.getLength(c);
      LOGGER.finest(() -> "Array length: " + length);

      final int[] elementsSize = {0};

      for (int i = 0; i < length; i++) {
        final int index = i;
        final Object element = Array.get(c, i);
        final int elementSize = staticSizeOf(element, classes);
        elementsSize[0] += elementSize;
        LOGGER.finest(() -> "Element " + index + " size: " + elementSize +
            ", type: " + (element != null ? element.getClass().getName() : "null"));
      }

      size += arrayHeaderSize[0] + elementsSize[0];
      final int finalElementsSize = elementsSize[0];
      final int finalSize = size; // Make a final copy for the lambda
      LOGGER.finest(() -> "Total array size: " + finalSize +
          " (header: " + arrayHeaderSize[0] + ", elements: " + finalElementsSize + ")");
    } else if (c instanceof String) {
      size += ((String) c).getBytes(UTF_8).length + 2; // 2 bytes for the length of the string
    } else if (c instanceof Optional<?> opt) {
      // 1 byte for the presence marker when empty
      // 1 byte for marker + size of contained value
      size += opt.map(o -> 1 + staticSizeOf(o, classes)).orElse(1);
    } else if (c instanceof Record record) {
      size += 4; // Length int (4 bytes)

      if (!classes.contains(record.getClass())) {
        // Add size for class name
        String className = record.getClass().getName();
        byte[] classNameBytes = className.getBytes(UTF_8);
        size += classNameBytes.length; // Class name bytes
        classes.add(record.getClass()); // Add class to seen classes
      }
      // Get the appropriate pickler for this record type
      @SuppressWarnings("unchecked")
      Pickler<Record> nestedPickler = (Pickler<Record>) Pickler.forRecord(record.getClass());
      size += nestedPickler.sizeOf(record); // Size of the record itself
    } else if (c instanceof Map<?, ?> map) {
      // 4 bytes for the number of entries
      size += 4;

      // Calculate size for each key-value pair
      for (Map.Entry<?, ?> entry : map.entrySet()) {
        // Add size of key
        size += staticSizeOf(entry.getKey(), classes);

        // Add size of value
        size += staticSizeOf(entry.getValue(), classes);
      }
    } else if (c instanceof List<?> list) {
      // 4 bytes for the number of entries
      size += 4;

      // Calculate size for each key-value pair
      for (var entry : list) {
        // Add size of key
        size += staticSizeOf(entry, classes);
      }
    } else if (c instanceof Enum<?> enumValue) {
      // Add size for enum class name
      if (!classes.contains(c.getClass())) {
        String enumClassName = c.getClass().getName();
        byte[] enumClassNameBytes = enumClassName.getBytes(UTF_8);
        int classNameSize = 4 + enumClassNameBytes.length; // 4 bytes for length + name bytes
        size += classNameSize;
        final int enumClassSize = classNameSize; // Make a final copy for the lambda
        LOGGER.finest(() -> "Enum class name size: " + enumClassSize +
            " for " + enumClassName);
        classes.add(c.getClass());
      } else {
        LOGGER.finest(() -> "Enum class already seen, using reference");
      }

      // Add size for enum constant name
      String enumConstantName = enumValue.name();
      byte[] enumNameBytes = enumConstantName.getBytes(UTF_8);
      int constantNameSize = 4 + enumNameBytes.length; // 4 bytes for length + name bytes
      size += constantNameSize;
      final int enumNameSize = constantNameSize; // Make a final copy for the lambda
      LOGGER.finest(() -> "Enum constant name size: " + enumNameSize +
          " for " + enumConstantName);
    } else {
      size += plainSize;
    }
    return size;
  }
}
