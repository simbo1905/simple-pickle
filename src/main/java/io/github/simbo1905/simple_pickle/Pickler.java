// SPDX-FileCopyrightText: 2025 Simon Massey
// SPDX-License-Identifier: Apache-2.0
package io.github.simbo1905.simple_pickle;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.reflect.Array;
import java.lang.reflect.RecordComponent;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import static io.github.simbo1905.simple_pickle.Pickler.Constants.*;
import static java.nio.charset.StandardCharsets.UTF_8;

/// Interface for serializing and deserializing objects record protocols.
///
/// @param <T> The type of object to be pickled which can be a record or a sealed interface of records.
public interface Pickler<T> {


  /// Registry to store Picklers by class to avoid redundant creation and infinite recursion
  ConcurrentHashMap<Class<?>, Pickler<?>> PICKLER_REGISTRY = new ConcurrentHashMap<>();

  /// Serializes an object into a byte buffer.
  ///
  /// @param object The object to serialize
  /// @param buffer The pre-allocated buffer to write to
  void serialize(T object, ByteBuffer buffer);

  /// Deserializes an object from a byte buffer.
  ///
  /// @param buffer The buffer to read from
  /// @return The deserialized object
  T deserialize(ByteBuffer buffer);

  /// Calculates the size in bytes required to serialize the given object
  ///
  /// This allows for pre-allocating buffers of the correct size without
  /// having to serialize the object first.
  ///
  /// @param value The object to calculate the size for
  /// @return The number of bytes required to serialize the object
  int sizeOf(T value);
  /// Get a Pickler for a record class, creating one if it doesn't exist in the registry
  @SuppressWarnings("unchecked")
  static <R extends Record> Pickler<R> picklerForRecord(Class<R> recordClass) {
    // Check if we already have a Pickler for this record class
    return (Pickler<R>) PICKLER_REGISTRY.computeIfAbsent(recordClass, clazz -> PicklerBase.createPicklerForRecord((Class<R>) clazz));
  }

  /// Get a Pickler for a record class, creating one if it doesn't exist in the registry
  /// The returned pickler creates record picklers for the permitted subclasses of the sealed interface
  @SuppressWarnings("unchecked")
  static <T> Pickler<T> picklerForSealedTrait(Class<T> sealedClass) {
    return (Pickler<T>) PICKLER_REGISTRY.computeIfAbsent(sealedClass, clazz -> PicklerBase.createPicklerForSealedTrait((Class<T>) clazz));
  }

  /// Base class for picklers that handle serialization and deserialization of Java records.
  /// Subclasses implement specific serialization logic for different data types while
  /// this base class provides common machinery for packing and unpacking to byte buffers.
  ///
  /// The machinery in this class is deliberately limited to the following simple "message protocol" types:
  ///
  /// - Primitive types
  /// - Strings
  /// - Optionals
  /// - Records that only contain the things above
  /// - Other records that only contain the things above
  /// - Arrays (primitive and object arrays or records that only match the things above)
  /// - Sealed interface hierarchies that only contain records that match the things above
  /// - Nested sealed interface hierarchies that only contain the things above
  ///
  /// Subclasses implement specific serialization logic for different data types while
  /// this base class provides common machinery for packing and unpacking to byte buffers.
  ///
  /// @see Pickler
  /// @see Record
  abstract class PicklerBase<R extends Record> implements Pickler<R> {
    public static final java.util.logging.Logger LOGGER =
        java.util.logging.Logger.getLogger(Pickler.class.getName());

    abstract Object[] staticGetComponents(R record);

    abstract R staticCreateFromComponents(Object[] record);

    /// The uses the abstract method to get the components of the record where we generated at runtime the method handles
    /// to all the accessor the record components using an anonymous subclass of PicklerBase.
    @Override
    public void serialize(R object, ByteBuffer buffer) {
      final var components = staticGetComponents(object);
      buffer.put((byte) components.length);
      Arrays.stream(components).forEach(c -> write(buffer, c));
    }

    @Override
    public int sizeOf(R object) {
      final var components = staticGetComponents(object);
      int size = 1; // Start with 1 byte for the type of the component
      for (Object c : components) {
        size += staticSizeOf(c);
        int finalSize = size;
        LOGGER.finer(() -> "Size of " +
            Optional.ofNullable(c).map(c2 -> c2.getClass().getSimpleName()).orElse("null")
            + " '" + c + "' is " + finalSize);
      }
      return size;
    }

    int staticSizeOf(Object c) {
      if (c == null) {
        return 1;
      }
      int plainSize = switch (c) {
        case Integer _ -> INTEGER.getSizeInBytes();
        case Long _ -> LONG.getSizeInBytes();
        case Short _ -> SHORT.getSizeInBytes();
        case Byte _ -> BYTE.getSizeInBytes();
        case Double _ -> DOUBLE.getSizeInBytes();
        case Float _ -> FLOAT.getSizeInBytes();
        case Character _ -> CHARACTER.getSizeInBytes();
        case Boolean _ -> BOOLEAN.getSizeInBytes();
        default -> 0;
      };
      int size = 1; // Type marker byte
      if (c.getClass().isArray()) {
        // Component type name size
        String componentTypeName = c.getClass().getComponentType().getName();
        byte[] componentTypeBytes = componentTypeName.getBytes(UTF_8);

        // Size includes: component type name length (4 bytes) + name bytes + array length (4 bytes)
        int arrayHeaderSize = 4 + componentTypeBytes.length + 4;

        // Calculate size of all array elements using streams
        int length = Array.getLength(c);
        int elementsSize = java.util.stream.IntStream.range(0, length)
            .map(i -> staticSizeOf(Array.get(c, i)))
            .sum();

        size += arrayHeaderSize + elementsSize;
      } else if (c instanceof String) {
        size += ((String) c).getBytes(UTF_8).length + 4; // 4 bytes for the length of the string
      } else if (c instanceof Optional<?> opt) {
        // 1 byte for the presence marker when empty
        // 1 byte for marker + size of contained value
        size += opt.map(o -> 1 + staticSizeOf(o)).orElse(1);
      } else if (c instanceof Record record) {
        // Add size for class name
        String className = record.getClass().getName();
        byte[] classNameBytes = className.getBytes(UTF_8);
        size += 4; // Length int (4 bytes)
        size += classNameBytes.length; // Class name bytes

        // Get the appropriate pickler for this record type
        @SuppressWarnings("unchecked")
        Pickler<Record> nestedPickler = (Pickler<Record>) Pickler.picklerForRecord(record.getClass());
        size += nestedPickler.sizeOf(record);
      } else {
        size += plainSize;
      }
      return size;
    }

    static byte typeMarker(Object c) {
      if (c == null) {
        return NULL.marker();
      }
      if (c.getClass().isArray()) {
        return ARRAY.marker();
      }
      return switch (c) {
        case Integer _ -> INTEGER.marker();
        case Long _ -> LONG.marker();
        case Short _ -> SHORT.marker();
        case Byte _ -> BYTE.marker();
        case Double _ -> DOUBLE.marker();
        case Float _ -> FLOAT.marker();
        case Character _ -> CHARACTER.marker();
        case Boolean _ -> BOOLEAN.marker();
        case String _ -> STRING.marker();
        case Optional<?> _ -> OPTIONAL.marker();
        case Record _ -> RECORD.marker();
        default -> throw new UnsupportedOperationException("Unsupported type: " + c.getClass());
      };
    }

    static void write(ByteBuffer buffer, Object c) {
      if (c == null) {
        buffer.put(NULL.marker());
        return;
      }

      if (c.getClass().isArray()) {
        buffer.put(ARRAY.marker());

        // Write the component type as a string
        String componentTypeName = c.getClass().getComponentType().getName();
        byte[] componentTypeBytes = componentTypeName.getBytes(UTF_8);
        buffer.putInt(componentTypeBytes.length);
        buffer.put(componentTypeBytes);

        // Write the array length
        int length = Array.getLength(c);
        buffer.putInt(length);

        // Write each element using IntStream instead of for loop
        java.util.stream.IntStream.range(0, length)
            .forEach(i -> write(buffer, Array.get(c, i)));
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
          buffer.putInt(bytes.length); // Using full int instead of byte
          buffer.put(bytes);
        }
        case Optional<?> opt -> {
          buffer.put(typeMarker(c));
          if (opt.isEmpty()) {
            buffer.put((byte) 0); // 0 = empty
          } else {
            buffer.put((byte) 1); // 1 = present
            Object value = opt.get();
            write(buffer, value);
          }
        }
        case Record record -> {
          buffer.put(typeMarker(c));
          // Write the class name for deserialization
          String className = record.getClass().getName();
          byte[] classNameBytes = className.getBytes(UTF_8);
          buffer.putInt(classNameBytes.length);
          buffer.put(classNameBytes);

          // Get the appropriate pickler for this record type
          @SuppressWarnings("unchecked")
          Pickler<Record> nestedPickler = (Pickler<Record>) Pickler.picklerForRecord(record.getClass());

          // Use that pickler to serialize the nested record
          nestedPickler.serialize(record, buffer);
        }
        default -> throw new UnsupportedOperationException("Unsupported type: " + c.getClass());
      }
    }

    /// The uses the abstract method to call the canonical constructor of the record via a method handle
    /// captured at runtime within an anonymous subclass of PicklerBase.
    @Override
    public R deserialize(ByteBuffer buffer) {
      final var length = buffer.get();
      final var components = new Object[length];
      Arrays.setAll(components, _ -> deserializeValue(buffer));
      return this.staticCreateFromComponents(components);
    }

    Object deserializeValue(ByteBuffer buffer) {
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
          final var strLength = buffer.getInt();
          final byte[] bytes = new byte[strLength];
          buffer.get(bytes);
          yield new String(bytes, UTF_8);
        }
        case OPTIONAL -> {
          byte isPresent = buffer.get();
          if (isPresent == 0) {
            yield Optional.empty();
          } else {
            Object value = deserializeValue(buffer);
            yield Optional.ofNullable(value);
          }
        }
        case RECORD -> { // Handle nested record
          // Read the class name
          int classNameLength = buffer.getInt();
          byte[] classNameBytes = new byte[classNameLength];
          buffer.get(classNameBytes);
          String className = new String(classNameBytes, UTF_8);

          try {
            // Load the class
            @SuppressWarnings("unchecked")
            Class<? extends Record> recordClass = (Class<? extends Record>) Class.forName(className);

            // Get or create the pickler for this class
            @SuppressWarnings("unchecked")
            Pickler<Record> nestedPickler = (Pickler<Record>) Pickler.picklerForRecord(recordClass);

            // Deserialize the nested record
            yield nestedPickler.deserialize(buffer);
          } catch (ClassNotFoundException e) {
            throw new RuntimeException("Failed to load class: " + className, e);
          }
        }
        case NULL -> null; // Handle null values
        case ARRAY -> { // Handle arrays
          // Read component type
          int componentTypeLength = buffer.getInt();
          byte[] componentTypeBytes = new byte[componentTypeLength];
          buffer.get(componentTypeBytes);
          String componentTypeName = new String(componentTypeBytes, UTF_8);

          // Read array length
          int length = buffer.getInt();

          try {
            // Get the component class
            Class<?> componentType = getClassForName(componentTypeName);

            // Create array of the right type and size
            Object array = Array.newInstance(componentType, length);

            // Deserialize each element using IntStream instead of for loop
            java.util.stream.IntStream.range(0, length)
                .forEach(i -> Array.set(array, i, deserializeValue(buffer)));

            yield array;
          } catch (ClassNotFoundException e) {
            throw new RuntimeException("Failed to load component type class: " + componentTypeName, e);
          }
        }
      };
    }

    /// Class.forName cannot handle primitive types directly, so we need to map them to their wrapper classes.
    Class<?> getClassForName(String name) throws ClassNotFoundException {
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

    /// Internal static method to create a new Pickler for a record class. This that creates the method handles for the
    /// record's component accessors and the canonical constructor.
    static <R extends Record> Pickler<R> createPicklerForRecord(Class<R> recordClass) {
      MethodHandle[] componentAccessors;
      MethodHandle constructorHandle;
      // Get lookup object
      MethodHandles.Lookup lookup = MethodHandles.lookup();
      // first we get the accessor method handles for the record components and add them to the array used
      // that is used by the base class to pull all the components out of the record to load into the byte buffer
      try {
        RecordComponent[] components = recordClass.getRecordComponents();
        componentAccessors = new MethodHandle[components.length];
        Arrays.setAll(componentAccessors, i -> {
          try {
            return lookup.unreflect(components[i].getAccessor());
          } catch (IllegalAccessException e) {
            throw new RuntimeException("Failed to access component: " + components[i].getName(), e);
          }
        });
      } catch (Exception e) {
        Throwable inner = e;
        while (inner.getCause() != null) {
          inner = inner.getCause();
        }
        throw new RuntimeException(inner.getMessage(), inner);
      }
      // Then we get the canonical constructor method handle for the record that will be used to create a new
      // instance from the components that the base class will pull out of the byte buffer
      try {
        // Get the record components
        RecordComponent[] components = recordClass.getRecordComponents();
        // Extract component types for the constructor
        Class<?>[] paramTypes = Arrays.stream(components).map(RecordComponent::getType).toArray(Class<?>[]::new);
        // Create method type for the canonical constructor
        MethodType constructorType = MethodType.methodType(void.class, paramTypes);

        constructorHandle = lookup.findConstructor(recordClass, constructorType);
      } catch (NoSuchMethodException | IllegalAccessException e) {
        throw new RuntimeException("Failed to access constructor for record: " + recordClass.getName(), e);
      }

      return new PicklerBase<>() {
        @Override
        Object[] staticGetComponents(R record) {
          Object[] result = new Object[componentAccessors.length];
          Arrays.setAll(result, i -> {
            try {
              return componentAccessors[i].invokeWithArguments(record);
            } catch (Throwable e) {
              throw new RuntimeException("Failed to access component: " + i, e);
            }
          });
          return result;
        }

        @SuppressWarnings("unchecked")
        @Override
        R staticCreateFromComponents(Object[] components) {
          try {
            return (R) constructorHandle.invokeWithArguments(components);
          } catch (Throwable e) {
            throw new RuntimeException(e);
          }
        }
      };
    }

    /// Creates an anonymous dispatcher pickler for a sealed interface. This will create a pickler for each permitted
    /// record.
    ///
    /// This method handles serialization and deserialization of null values and
    /// concrete type instances. Here's the step-by-step process:
    ///
    /// 1. Creates a pickler that handles the sealed interface type
    /// 2. Special handling for null values (type marker 11)
    /// 3. Writes the class name for proper deserialization
    /// 4. Retrieves the appropriate pickler for the concrete type
    /// 5. Uses the specific pickler to serialize the object
    /// 6. Checks for null values and consumes the null marker (11)
    /// 7. Reads and loads the class name for deserialization
    /// 8. Loads the concrete class and gets its pickler
    /// 9. Deserializes the object using the concrete pickler
    ///
    /// *Important*: Handles null values and type safety through class name tracking
    ///
    /// @return a new pickler instance for the sealed interface type
    ///
    /// Example usage:
    /// ```java
    /// Pickler<Object> pickler = new Pickler<Object>(){
    ///     // Implementation
    ///};
    ///```
    static <T> Pickler<?> createPicklerForSealedTrait(Class<T> sealedClass) {
      @SuppressWarnings("unchecked") Pickler<T> existingPickler = (Pickler<T>) PICKLER_REGISTRY.get(sealedClass);
      if (existingPickler != null) {
        return existingPickler;
      }

      // Pre-register all subclass picklers
      Class<?>[] subclasses = sealedClass.getPermittedSubclasses();
      //noinspection unchecked
      Arrays.stream(subclasses).filter(Record.class::isAssignableFrom).forEach(permitted -> createPicklerForRecord((Class<? extends Record>) permitted));

      return new Pickler<T>() {
        @Override
        public void serialize(T object, ByteBuffer buffer) {
          if (object == null) {
            LOGGER.fine(() -> "Serializing null object");
            buffer.put(NULL.marker());
            return;
          }

          // Write the class name for deserialization
          String className = object.getClass().getName();
          byte[] classNameBytes = className.getBytes(UTF_8);
          buffer.putInt(classNameBytes.length);
          buffer.put(classNameBytes);

          // Get the appropriate pickler for this concrete type
          @SuppressWarnings("unchecked") Pickler<Record> concretePickler = (Pickler<Record>) picklerForRecord((Class<? extends Record>) object.getClass());

          LOGGER.fine(() -> "Serializing " + className + " with pickler: " + concretePickler.hashCode());

          // Use that pickler to serialize the object
          concretePickler.serialize((Record) object, buffer);
        }

        @Override
        public T deserialize(ByteBuffer buffer) {
          // Check if the value is null
          byte firstByte = buffer.get(buffer.position());
          if (firstByte == NULL.marker()) {
            buffer.get(); // Consume the null marker
            LOGGER.fine(() -> "Deserialized null object");
            return null;
          }

          // Read the class name
          int classNameLength = buffer.getInt();
          byte[] classNameBytes = new byte[classNameLength];
          buffer.get(classNameBytes);
          String className = new String(classNameBytes, UTF_8);

          try {
            // Load the class
            @SuppressWarnings("unchecked") Class<? extends Record> concreteClass = (Class<? extends Record>) Class.forName(className);

            // Get the pickler for this concrete class
            @SuppressWarnings("unchecked") Pickler<Record> concretePickler = (Pickler<Record>) picklerForRecord(concreteClass);

            LOGGER.fine(() -> "Deserializing " + className + " with pickler: " + concretePickler.hashCode());

            // Deserialize using the concrete pickler
            @SuppressWarnings("unchecked") T result = (T) concretePickler.deserialize(buffer);

            return result;
          } catch (ClassNotFoundException e) {
            throw new RuntimeException("Failed to load class: " + className, e);
          }
        }

        @Override
        public int sizeOf(T value) {
          if (value == null) return 1;

          // Get the class name length
          String className = value.getClass().getName();
          byte[] classNameBytes = className.getBytes(UTF_8);
          int classNameLength = classNameBytes.length;

          // Get the pickler for this concrete type
          @SuppressWarnings("unchecked") Pickler<Record> concretePickler = (Pickler<Record>) picklerForRecord((Class<? extends Record>) value.getClass());

          // Calculate the size using the concrete pickler
          // 1 byte for type marker + 4 bytes for class name length + class name bytes + concrete size
          return 4 + classNameLength + concretePickler.sizeOf((Record) value);
        }
      };
    }
  }
  
  /// Enum containing constants used throughout the Pickler implementation
  enum Constants {
    NULL((byte) 1, 0, null),
    BOOLEAN((byte) 2, 1, boolean.class),
    BYTE((byte)3, Byte.BYTES, byte.class),
    SHORT((byte) 4, Short.BYTES, short.class),
    CHARACTER((byte) 5, Character.BYTES, char.class),
    INTEGER((byte) 6, Integer.BYTES, int.class),
    LONG((byte) 7, Long.BYTES, long.class),
    FLOAT((byte) 8, Float.BYTES, float.class),
    DOUBLE((byte) 9, Double.BYTES, double.class),
    STRING((byte) 10, 0, String.class),
    OPTIONAL((byte) 11, 0, Optional.class),
    RECORD((byte) 12, 0, Record.class),
    ARRAY((byte) 13, 0, null);

    private final byte typeMarker;
    private final int sizeInBytes;
    private final Class<?> primitiveClass;

    Constants(byte typeMarker, int sizeInBytes, Class<?> primitiveClass) {
      this.typeMarker = typeMarker;
      this.sizeInBytes = sizeInBytes;
      this.primitiveClass = primitiveClass;
    }

    public byte marker() {
      return typeMarker;
    }

    public int getSizeInBytes() {
      return sizeInBytes;
    }

    public Class<?> _class() {
      return primitiveClass;
    }

    public static Constants fromMarker(byte marker) {
      for (Constants c : values()) {
        if (c.typeMarker == marker) {
          return c;
        }
      }
      throw new IllegalArgumentException("Unknown type marker: " + marker);
    }
  }
}
