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

import static java.nio.charset.StandardCharsets.UTF_8;

/// Interface for serializing and deserializing objects record protocols.
///
/// TODO manufacture an estimate size of the object to be pickled
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

    static byte typeMarker(Object c) {
      if (c == null) {
        return 11; // 11 is for null values
      }
      if (c.getClass().isArray()) {
        return 12; // 12 is for arrays
      }
      return switch (c) {
        case Integer _ -> 0;
        case Long _ -> 1;
        case Short _ -> 2;
        case Byte _ -> 3;
        case Double _ -> 4;
        case Float _ -> 5;
        case Character _ -> 6;
        case Boolean _ -> 7;
        case String _ -> 8;
        case Optional<?> _ -> 9;
        case Record _ -> 10; // 10 is for nested records
        default -> throw new UnsupportedOperationException("Unsupported type: " + c.getClass());
      };
    }

    static void write(ByteBuffer buffer, Object c) {
      if (c == null) {
        buffer.put((byte) 11); // 11 is for null values
        return;
      }

      if (c.getClass().isArray()) {
        buffer.put((byte) 12); // 12 is for arrays

        // Write the component type as a string
        String componentTypeName = c.getClass().getComponentType().getName();
        byte[] componentTypeBytes = componentTypeName.getBytes(UTF_8);
        buffer.put((byte) componentTypeBytes.length);
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
          buffer.put((byte) bytes.length);
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
          buffer.put((byte) classNameBytes.length);
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
      return switch (type) {
        case 0 -> buffer.getInt();
        case 1 -> buffer.getLong();
        case 2 -> buffer.getShort();
        case 3 -> buffer.get();
        case 4 -> buffer.getDouble();
        case 5 -> buffer.getFloat();
        case 6 -> buffer.getChar();
        case 7 -> buffer.get() == 1;
        case 8 -> {
          final var strLength = buffer.get();
          final byte[] bytes = new byte[strLength];
          buffer.get(bytes);
          yield new String(bytes, UTF_8);
        }
        case 9 -> {
          byte isPresent = buffer.get();
          if (isPresent == 0) {
            yield Optional.empty();
          } else {
            Object value = deserializeValue(buffer);
            yield Optional.ofNullable(value);
          }
        }
        case 10 -> { // Handle nested record
          // Read the class name
          byte classNameLength = buffer.get();
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
        case 11 -> null; // Handle null values
        case 12 -> { // Handle arrays
          // Read component type
          byte componentTypeLength = buffer.get();
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
        default -> throw new UnsupportedOperationException("Unsupported type: " + type);
      };
    }

    /// Class.forName cannot handle primitive types directly, so we need to map them to their wrapper classes.
    Class<?> getClassForName(String name) throws ClassNotFoundException {
      // Handle primitive types which can't be loaded directly with Class.forName
      return switch (name) {
        case "boolean" -> boolean.class;
        case "byte" -> byte.class;
        case "char" -> char.class;
        case "short" -> short.class;
        case "int" -> int.class;
        case "long" -> long.class;
        case "float" -> float.class;
        case "double" -> double.class;
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
      // final we get the canonical constructor method handle for the record that will be used to create a new
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
            buffer.put((byte) 11); // 11 is for null values
            return;
          }

          // Write the class name for deserialization
          String className = object.getClass().getName();
          byte[] classNameBytes = className.getBytes(UTF_8);
          buffer.put((byte) classNameBytes.length);
          buffer.put(classNameBytes);

          // Get the appropriate pickler for this concrete type
          @SuppressWarnings("unchecked") Pickler<Record> concretePickler = (Pickler<Record>) picklerForRecord((Class<? extends Record>) object.getClass());

          // Use that pickler to serialize the object
          concretePickler.serialize((Record) object, buffer);
        }

        @Override
        public T deserialize(ByteBuffer buffer) {
          // Check if the value is null (type marker 11)
          byte firstByte = buffer.get(buffer.position());
          if (firstByte == 11) {
            buffer.get(); // Consume the null marker
            return null;
          }

          // Read the class name
          byte classNameLength = buffer.get();
          byte[] classNameBytes = new byte[classNameLength];
          buffer.get(classNameBytes);
          String className = new String(classNameBytes, UTF_8);

          try {
            // Load the class
            @SuppressWarnings("unchecked") Class<? extends Record> concreteClass = (Class<? extends Record>) Class.forName(className);

            // Get the pickler for this concrete class
            @SuppressWarnings("unchecked") Pickler<Record> concretePickler = (Pickler<Record>) picklerForRecord(concreteClass);

            // Deserialize using the concrete pickler
            @SuppressWarnings("unchecked") T result = (T) concretePickler.deserialize(buffer);

            return result;
          } catch (ClassNotFoundException e) {
            throw new RuntimeException("Failed to load class: " + className, e);
          }
        }
      };
    }
  }
}
