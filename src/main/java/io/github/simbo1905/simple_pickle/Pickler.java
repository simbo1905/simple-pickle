// SPDX-FileCopyrightText: 2025 Simon Massey
// SPDX-License-Identifier: Apache-2.0
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
import java.util.stream.IntStream;

import static io.github.simbo1905.simple_pickle.Pickler.Constants.*;
import static java.nio.charset.StandardCharsets.UTF_8;

/// Interface for serializing and deserializing objects record protocols.
///
/// @param <T> The type of object to be pickled which can be a record or a sealed interface of records.
public interface Pickler<T> {
  /// Logger for the Pickler interface
  java.util.logging.Logger LOGGER = java.util.logging.Logger.getLogger(Pickler.class.getName());


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
    return (Pickler<R>) PICKLER_REGISTRY.computeIfAbsent(recordClass, clazz -> {
      // Validate record components for enum types
      validateRecordComponents((Class<R>) clazz);
      return PicklerBase.createPicklerForRecord((Class<R>) clazz);
    });
  }
  
  /**
   * Validates that all enum types used in record components are simple enums.
   * A simple enum has no instance fields, no custom constructors, and no class bodies.
   * 
   * @param recordClass The record class to validate
   * @throws UnsupportedOperationException if a complex enum is found
   */
  static <R extends Record> void validateRecordComponents(Class<R> recordClass) {
    RecordComponent[] components = recordClass.getRecordComponents();
    for (RecordComponent component : components) {
      Class<?> type = component.getType();
      
      // Check if the type is an enum
      if (type.isEnum()) {
        validateSimpleEnum(type);
      }
      
      // Check if it's an array of enums
      if (type.isArray() && type.getComponentType().isEnum()) {
        validateSimpleEnum(type.getComponentType());
      }
      
    }
  }
  
  /**
   * Helper method to get an enum constant with proper type witness
   * 
   * @param enumClass The enum class
   * @param enumName The name of the enum constant
   * @return The enum constant
   */
  @SuppressWarnings("unchecked")
  private static <E extends Enum<E>> Object enumValueOf(Class<?> enumClass, String enumName) {
      return Enum.valueOf((Class<E>) enumClass, enumName);
  }
  
  /**
   * Validates that an enum is a simple enum without custom fields, methods, or constructors.
   * 
   * @param enumClass The enum class to validate
   * @throws UnsupportedOperationException if the enum is complex
   */
  private static void validateSimpleEnum(Class<?> enumClass) {
    if (!enumClass.isEnum()) {
      return;
    }
    
    // Check for instance fields (excluding compiler-generated ones)
    java.lang.reflect.Field[] fields = enumClass.getDeclaredFields();
    for (java.lang.reflect.Field field : fields) {
      // Skip static fields, synthetic fields, and the compiler-generated $VALUES field
      if (java.lang.reflect.Modifier.isStatic(field.getModifiers()) || 
          field.isSynthetic() || 
          field.getName().equals("$VALUES")) {
        continue;
      }
      
      // Skip the compiler-generated enum constant fields
      if (field.getName().equals("name") || field.getName().equals("ordinal")) {
        continue;
      }
      
      // If we found any other instance field, this is a complex enum
      throw new UnsupportedOperationException(
          "Complex enum not supported: " + enumClass.getName() + 
          " has custom field: " + field.getName());
    }
    
    // Check for custom constructors
    java.lang.reflect.Constructor<?>[] constructors = enumClass.getDeclaredConstructors();
    for (java.lang.reflect.Constructor<?> constructor : constructors) {
      // Skip synthetic constructors
      if (constructor.isSynthetic()) {
        continue;
      }
      
      // The default enum constructor has 2 parameters: name and ordinal
      if (constructor.getParameterCount() > 2) {
        throw new UnsupportedOperationException(
            "Complex enum not supported: " + enumClass.getName() + 
            " has custom constructor with " + constructor.getParameterCount() + " parameters");
      }
    }
    
    // Check for enum constants with class bodies
    Object[] constants = enumClass.getEnumConstants();
    for (Object constant : constants) {
      if (constant.getClass() != enumClass) {
        throw new UnsupportedOperationException(
            "Complex enum not supported: " + enumClass.getName() + 
            " has enum constant with class body: " + ((Enum<?>)constant).name());
      }
    }
  }

  /// Get a Pickler for a record class, creating one if it doesn't exist in the registry
  /// The returned pickler creates record picklers for the permitted subclasses of the sealed interface
  @SuppressWarnings("unchecked")
  static <T> Pickler<T> picklerForSealedTrait(Class<T> sealedClass) {
    return (Pickler<T>) PICKLER_REGISTRY.computeIfAbsent(sealedClass, clazz -> PicklerBase.createPicklerForSealedTrait((Class<T>) clazz));
  }
  
  /// Serialize an array of records
  ///
  /// @param array The array of records to serialize
  /// @param buffer The buffer to write to
  /// @param <R> The record type
  static <R extends Record> void serializeArray(R[] array, ByteBuffer buffer) {
    // Get the pickler for the component type
    @SuppressWarnings("unchecked")
    Pickler<R> pickler = picklerForRecord((Class<R>) array.getClass().getComponentType());
    
    // Write array marker
    buffer.put(Constants.ARRAY.marker());
    
    // Write component type
    Map<Class<?>, Integer> class2BufferOffset = new HashMap<>();
    writeClassNameWithDeduplication(buffer, array.getClass().getComponentType(), class2BufferOffset);
    
    // Write array length
    buffer.putInt(array.length);
    
    // Write each element
    for (R element : array) {
      pickler.serialize(element, buffer);
    }
  }
  
  /// Deserialize an array of records
  ///
  /// @param buffer The buffer to read from
  /// @param componentType The component type of the array
  /// @param <R> The record type
  /// @return The deserialized array
  @SuppressWarnings("unchecked")
  static <R extends Record> R[] deserializeArray(ByteBuffer buffer, Class<R> componentType) {
    // Skip the array marker if present
    if (buffer.get(buffer.position()) == Constants.ARRAY.marker()) {
      buffer.get(); // Consume the marker
      
      // Read component type
      Map<Integer, Class<?>> bufferOffset2Class = new HashMap<>();
      try {
        Class<?> readComponentType = readClassNameWithDeduplication(buffer, bufferOffset2Class);
        if (!componentType.equals(readComponentType)) {
          throw new IllegalArgumentException("Component type mismatch: expected " + 
              componentType.getName() + " but got " + readComponentType.getName());
        }
      } catch (ClassNotFoundException e) {
        throw new RuntimeException("Failed to load component type class", e);
      }
    } else {
      // If no array marker, rewind to original position
      buffer.position(buffer.position() - 1);
    }
    
    // Read array length
    int length = buffer.getInt();
    
    // Create array of the right type and size
    R[] array = (R[]) Array.newInstance(componentType, length);
    
    // Get the pickler for the component type
    Pickler<R> pickler = picklerForRecord(componentType);
    
    // Deserialize each element
    for (int i = 0; i < length; i++) {
      array[i] = pickler.deserialize(buffer);
    }
    
    return array;
  }
  
  /// Calculate the size of an array of records
  ///
  /// @param array The array of records
  /// @param <R> The record type
  /// @return The size in bytes
  static <R extends Record> int sizeOfArray(R[] array) {
    if (array == null) {
      return 1; // Just the NULL marker
    }
    
    // Start with 1 byte for the ARRAY marker
    int size = 1;
    
    // Add size for component type name (4 bytes for length + name bytes)
    String componentTypeName = array.getClass().getComponentType().getName();
    size += 4 + componentTypeName.getBytes(UTF_8).length;
    
    // Add 4 bytes for array length
    size += 4;
    
    // Get the pickler for the component type
    @SuppressWarnings("unchecked")
    Pickler<R> pickler = picklerForRecord((Class<R>) array.getClass().getComponentType());
    
    // Add size of each element
    for (R element : array) {
      size += pickler.sizeOf(element);
    }
    
    return size;
  }
  
  /**
   * Helper method to write a class name to a buffer with deduplication.
   * If the class has been seen before, writes a negative reference instead of the full name.
   * 
   * @param buffer The buffer to write to
   * @param clazz The class to write
   * @param class2BufferOffset Map tracking class to buffer position
   */
  static void writeClassNameWithDeduplication(ByteBuffer buffer, Class<?> clazz,
                                              Map<Class<?>, Integer> class2BufferOffset) {
    LOGGER.finest(() -> "writeClassNameWithDeduplication: class=" + clazz.getName() +
        ", buffer position=" + buffer.position() +
        ", map contains class=" + class2BufferOffset.containsKey(clazz));

    // Check if we've seen this class before
    Integer existingOffset = class2BufferOffset.get(clazz);
    if (existingOffset != null) {
      // We've seen this class before, write a negative reference
      int reference = ~existingOffset;
      buffer.putInt(reference); // Using bitwise complement for negative reference
      LOGGER.finest(() -> "Wrote class reference: " + clazz.getName() +
          " at offset " + existingOffset + ", value=" + reference);
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
      class2BufferOffset.put(clazz, currentPosition);
      LOGGER.finest(() -> "Wrote class name: " + className +
          " at offset " + currentPosition +
          ", length=" + classNameLength +
          ", new buffer position=" + buffer.position());
    }
  }
  
  /**
   * Helper method to read a class name from a buffer with deduplication support.
   * 
   * @param buffer The buffer to read from
   * @param bufferOffset2Class Map tracking buffer position to class
   * @return The loaded class
   */
  static Class<?> readClassNameWithDeduplication(ByteBuffer buffer,
                                                 Map<Integer, Class<?>> bufferOffset2Class)
      throws ClassNotFoundException {
    int bufferPosition = buffer.position();

    LOGGER.finest(() -> "readClassNameWithDeduplication: buffer position=" + bufferPosition +
        ", remaining=" + buffer.remaining());

    // Read the class name length or reference
    int componentTypeLength = buffer.getInt();

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
        throw new RuntimeException("Invalid class reference at offset: " + offset +
            ", available offsets: " + bufferOffset2Class.keySet());
      }
      return referencedClass;
    } else {
      // This is a new class name
      int currentPosition = buffer.position() - 4; // Position before reading the length

      LOGGER.finest(() -> "New class name detected. Length=" + componentTypeLength +
          ", stored position=" + currentPosition +
          ", remaining bytes=" + buffer.remaining());

      if (buffer.remaining() < componentTypeLength) {
        LOGGER.severe(() -> "Buffer underflow imminent: needed " + componentTypeLength +
            " bytes but only " + buffer.remaining() + " remaining");
        throw new IllegalArgumentException("Buffer underflow: class name length (" +
            componentTypeLength + ") exceeds remaining buffer size (" + buffer.remaining() + ")");
      }

      // Read the class name
      byte[] classNameBytes = new byte[componentTypeLength];
      buffer.get(classNameBytes);
      String className = new String(classNameBytes, UTF_8);

      // Validate class name - add basic validation that allows array type names like `[I`, `[[I`, `[L`java.lang.String;` etc.
      if (!className.matches("[\\[\\]a-zA-Z0-9_.$;]+")) {
        LOGGER.severe("Invalid class name format: " + className);
        throw new IllegalArgumentException("Invalid class name format: " + className);
      }

      // Load the class using our helper method
      Class<?> loadedClass = PicklerBase.getClassForName(className);

      // Store in our map for future references
      bufferOffset2Class.put(currentPosition, loadedClass);
      LOGGER.finest(() -> "Read class name at offset " + currentPosition +
          ": " + className +
          ", new buffer position=" + buffer.position());

      return loadedClass;
    }
  }
  interface PicklerInternal<T> extends Pickler<T> {
    void serialize(T object, ByteBuffer buffer, Map<Class<?>, Integer> class2BufferOffset);

    T deserialize(ByteBuffer buffer, Map<Integer, Class<?>> bufferOffset2Class);

    int sizeOf(T object, Set<Class<?>> classes);
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
  abstract class PicklerBase<R extends Record> implements PicklerInternal<R> {

    abstract Object[] staticGetComponents(R record);

    abstract R staticCreateFromComponents(Object[] record);

    /// The uses the abstract method to get the components of the record where we generated at runtime the method handles
    /// to all the accessor the record components using an anonymous subclass of PicklerBase.
    @Override
    public void serialize(R object, ByteBuffer buffer) {
      serialize(object, buffer, new HashMap<>());
    }

    @Override
    public void serialize(R object, ByteBuffer buffer, Map<Class<?>, Integer> class2BufferOffset) {
      final var components = staticGetComponents(object);
      buffer.put((byte) components.length);
      Arrays.stream(components).forEach(c -> write(class2BufferOffset, buffer, c));
    }

    @Override
    public int sizeOf(R object) {
      return sizeOf(object, new HashSet<>());
    }

    @Override
    public int sizeOf(R object, Set<Class<?>> classes) {
      final var components = staticGetComponents(object);
      int size = 1; // Start with 1 byte for the type of the component
      for (Object c : components) {
        size += staticSizeOf(c, classes);
        int finalSize = size;
        LOGGER.finer(() -> "Size of " +
            Optional.ofNullable(c).map(c2 -> c2.getClass().getSimpleName()).orElse("null")
            + " '" + c + "' is " + finalSize);
      }
      return size;
    }

    int staticSizeOf(Object c, Set<Class<?>> classes) {
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
        size += ((String) c).getBytes(UTF_8).length + 4; // 4 bytes for the length of the string
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
        PicklerInternal<Record> nestedPickler = (PicklerInternal<Record>) Pickler.picklerForRecord(record.getClass());
        size += nestedPickler.sizeOf(record, classes); // Size of the record itself
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
        default -> throw new UnsupportedOperationException("Unsupported type: " + c.getClass());
      };
    }

    static void write(Map<Class<?>, Integer> class2BufferOffset, ByteBuffer buffer, Object c) {
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

        writeClassNameWithDeduplication(buffer, c.getClass().getComponentType(), class2BufferOffset);

        // Write the array length
        int length = Array.getLength(c);
        buffer.putInt(length);

        IntStream.range(0, length).forEach(i -> write(class2BufferOffset, buffer, Array.get(c, i)));
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
            write(class2BufferOffset, buffer, value);
          }
        }
        case Record record -> {
          buffer.put(typeMarker(c));
              
          // Write the class name with deduplication
          writeClassNameWithDeduplication(buffer, record.getClass(), class2BufferOffset);

          // Get the appropriate pickler for this record type
          @SuppressWarnings("unchecked")
          PicklerInternal<Record> nestedPickler = (PicklerInternal<Record>) Pickler.picklerForRecord(record.getClass());

          // Use that pickler to serialize the nested record
          nestedPickler.serialize(record, buffer, class2BufferOffset);
        }
        case Map<?, ?> map -> {
          buffer.put(typeMarker(c));
          
          // Write the number of entries
          buffer.putInt(map.size());
          
          // Write each key-value pair
          map.forEach((key, value) -> {
            // Write the key
            write(class2BufferOffset, buffer, key);
            // Write the value
            write(class2BufferOffset, buffer, value);
          });
        }
        case Enum<?> enumValue -> {
          int enumStartPos = buffer.position();
          LOGGER.finest(() -> "Starting enum serialization at position " + enumStartPos);
          
          buffer.put(typeMarker(c));
          LOGGER.finest(() -> "Wrote enum type marker: " + ENUM.marker());
          
          // Write the enum class name with deduplication
          int beforeClassName = buffer.position();
          writeClassNameWithDeduplication(buffer, enumValue.getClass(), class2BufferOffset);
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
        default -> throw new UnsupportedOperationException("Unsupported type: " + c.getClass());
      }
    }

    /// The uses the abstract method to call the canonical constructor of the record via a method handle
    /// captured at runtime within an anonymous subclass of PicklerBase.
    @Override
    public R deserialize(ByteBuffer buffer) {
      return deserialize(buffer, new HashMap<>());
    }

    @Override
    public R deserialize(ByteBuffer buffer, Map<Integer, Class<?>> bufferOffset2Class) {
      final var length = buffer.get();
      final var components = new Object[length];
      Arrays.setAll(components, ignored -> deserializeValue(bufferOffset2Class, buffer));
      return this.staticCreateFromComponents(components);
    }

    Object deserializeValue(Map<Integer, Class<?>> bufferOffset2Class, ByteBuffer buffer) {
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
            Object value = deserializeValue(bufferOffset2Class, buffer);
            yield Optional.ofNullable(value);
          }
        }
        case RECORD -> { // Handle nested record
          try {
            // Read the class with deduplication support
            Class<?> recordClass = readClassNameWithDeduplication(buffer, bufferOffset2Class);

            // Get or create the pickler for this class
            @SuppressWarnings("unchecked")
            PicklerInternal<Record> nestedPickler = (PicklerInternal<Record>) Pickler.picklerForRecord((Class<? extends Record>) recordClass);

            // Deserialize the nested record
            yield nestedPickler.deserialize(buffer, bufferOffset2Class);
          } catch (ClassNotFoundException e) {
            throw new RuntimeException("Failed to load class", e);
          }
        }
        case NULL -> null; // Handle null values
        case ARRAY -> { // Handle arrays
          try {
            // Get the component class
            Class<?> componentType = readClassNameWithDeduplication(buffer, bufferOffset2Class);
            
            // Read array length
            int length = buffer.getInt();

            // Create array of the right type and size
            final Object array = Array.newInstance(componentType, length);

            // Deserialize each element using IntStream instead of for loop
            IntStream.range(0, length)
                .forEach(i -> Array.set(array, i, deserializeValue(bufferOffset2Class, buffer)));

            yield array;
          } catch (ClassNotFoundException e) {
            throw new RuntimeException("Failed to load component type class: ", e);
          }
        }
        case MAP -> { // Handle maps
          // Read the number of entries
          int size = buffer.getInt();
          
          // Create a new HashMap to hold the entries
          Map<Object, Object> map = new HashMap<>(size);
          
          // Read each key-value pair
          for (int i = 0; i < size; i++) {
            // Read the key
            Object key = deserializeValue(bufferOffset2Class, buffer);
            
            // Read the value
            Object value = deserializeValue(bufferOffset2Class, buffer);
            
            // Add to the map
            map.put(key, value);
          }
          
          yield map;
        }
        case ENUM -> { // Handle enums
          try {
            // Read the enum class with deduplication support
            Class<?> enumClass = readClassNameWithDeduplication(buffer, bufferOffset2Class);
            
            // Verify it's an enum class
            if (!enumClass.isEnum()) {
              throw new IllegalArgumentException("Expected enum class but got: " + enumClass.getName());
            }
            
            // Read the enum constant name
            int enumNameLength = buffer.getInt();
            byte[] enumNameBytes = new byte[enumNameLength];
            buffer.get(enumNameBytes);
            String enumName = new String(enumNameBytes, UTF_8);

            // Get the enum constant using helper method with proper type witness
            yield enumValueOf(enumClass, enumName);
          } catch (ClassNotFoundException e) {
            throw new RuntimeException("Failed to load enum class", e);
          }
        }
      };
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

    /// Internal static method to create a new Pickler for a record class. This creates the method handles for the
    /// record's component accessors, the canonical constructor, and any fallback constructors for schema evolution.
    static <R extends Record> PicklerInternal<R> createPicklerForRecord(Class<R> recordClass) {
      MethodHandle[] componentAccessors;
      MethodHandle canonicalConstructorHandle;
      Map<Integer, MethodHandle> fallbackConstructorHandles = new HashMap<>();
      int canonicalParamCount;
    
      // Get lookup object
      MethodHandles.Lookup lookup = MethodHandles.lookup();
    
      // First we get the accessor method handles for the record components and add them to the array used
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
    
      // Get the canonical constructor and any fallback constructors for schema evolution
      try {
        // Get the record components
        RecordComponent[] components = recordClass.getRecordComponents();
        // Extract component types for the canonical constructor
        Class<?>[] canonicalParamTypes = Arrays.stream(components)
            .map(RecordComponent::getType)
            .toArray(Class<?>[]::new);
        canonicalParamCount = canonicalParamTypes.length;
      
        // Get all public constructors
        Constructor<?>[] allConstructors = recordClass.getConstructors();
      
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
            throw new RuntimeException("Failed to access canonical constructor for record: " + 
                recordClass.getName(), e);
          }
        }
      } catch (Exception e) {
        throw new RuntimeException("Failed to access constructors for record: " + 
            recordClass.getName(), e);
      }
    
      // Capture these values for use in the anonymous class
      final MethodHandle finalCanonicalConstructorHandle = canonicalConstructorHandle;
      final int finalCanonicalParamCount = canonicalParamCount;
      final String recordClassName = recordClass.getName();
      final Map<Integer, MethodHandle> finalFallbackConstructorHandles = 
          Collections.unmodifiableMap(fallbackConstructorHandles);

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
            // Get the number of components from the serialized data
            int numComponents = components.length;
            MethodHandle constructorToUse;
            
            if (numComponents == finalCanonicalParamCount) {
              // Number of components matches the canonical constructor - use it directly
              constructorToUse = finalCanonicalConstructorHandle;
              LOGGER.finest(() -> "Using canonical constructor for " + recordClassName + 
                  " with " + numComponents + " components");
            } else {
              // Number of components differs, look for a fallback constructor
              constructorToUse = finalFallbackConstructorHandles.get(numComponents);
              if (constructorToUse == null) {
                // No fallback constructor matches the number of components found
                throw new RuntimeException("Schema evolution error: Cannot deserialize data for " + 
                    recordClassName + ". Found " + numComponents + 
                    " components, but no matching constructor (canonical or fallback) exists.");
              }
              LOGGER.finest(() -> "Using fallback constructor for " + recordClassName + 
                  " with " + numComponents + " components");
            }
            
            // Invoke the selected constructor
            return (R) constructorToUse.invokeWithArguments(components);
          } catch (Throwable e) {
            throw new RuntimeException("Failed to create instance of " + recordClassName, e);
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

      return new PicklerInternal<T>() {
        @Override
        public void serialize(T object, ByteBuffer buffer) {
          // Create a map to track class name positions for this serialization session
          Map<Class<?>, Integer> class2BufferOffset = new HashMap<>();
          serialize(object, buffer, class2BufferOffset);
        }

        @Override
        public void serialize(T object, ByteBuffer buffer, Map<Class<?>, Integer> class2BufferOffset) {
          if (object == null) {
            LOGGER.fine(() -> "Serializing null object");
            buffer.put(NULL.marker());
            return;
          }

          // Write the class name with deduplication
          writeClassNameWithDeduplication(buffer, object.getClass(), class2BufferOffset);

          // Get the appropriate pickler for this concrete type
          @SuppressWarnings("unchecked") PicklerInternal<Record> concretePickler =
              (PicklerInternal<Record>) picklerForRecord((Class<? extends Record>) object.getClass());

          LOGGER.fine(() -> "Serializing " + object.getClass().getName() + 
                     " with pickler: " + concretePickler.hashCode());

          // Use that pickler to serialize the object
          concretePickler.serialize((Record) object, buffer, class2BufferOffset);
        }

        @Override
        public T deserialize(ByteBuffer buffer) {
          // Create a map to track class positions for this deserialization session
          Map<Integer, Class<?>> bufferOffset2Class = new java.util.HashMap<>();
          return deserialize(buffer, bufferOffset2Class);
        }

        @Override
        public T deserialize(ByteBuffer buffer, Map<Integer, Class<?>> bufferOffset2Class) {
          // Check if the value is null
          byte firstByte = buffer.get(buffer.position());
          if (firstByte == NULL.marker()) {
            buffer.get(); // Consume the null marker
            LOGGER.fine(() -> "Deserialized null object");
            return null;
          }

          try {
            // Read the class name with deduplication support
            Class<?> concreteClass = readClassNameWithDeduplication(buffer, bufferOffset2Class);

            // Get the pickler for this concrete class
            @SuppressWarnings("unchecked") PicklerInternal<Record> concretePickler =
                (PicklerInternal<Record>) picklerForRecord((Class<? extends Record>) concreteClass);

            LOGGER.fine(() -> "Deserializing " + concreteClass.getName() + 
                       " with pickler: " + concretePickler.hashCode());

            // Deserialize using the concrete pickler
            @SuppressWarnings("unchecked") T result = (T) concretePickler.deserialize(buffer, bufferOffset2Class);

            return result;
          } catch (ClassNotFoundException e) {
            throw new RuntimeException("Failed to load class", e);
          }
        }

        @Override
        public int sizeOf(T value) {
          return sizeOf(value, new HashSet<>());
        }

        @Override
        public int sizeOf(T value, Set<Class<?>> classes) {
          return sizeOfWithDeduplication(value, classes);
        }

        private int sizeOfWithDeduplication(T value, Set<Class<?>> classes) {
          if (value == null) {
            return 1;
          }

          // 4 bytes for length
          int size = 4;
          
          // Check if we've seen this class before
          if (!classes.contains(value.getClass())) {
            // First time seeing this class, need full class name
            String className = value.getClass().getName();
            byte[] classNameBytes = className.getBytes(UTF_8);
            int classNameLength = classNameBytes.length;

            size += classNameLength;
            // Mark this class as seen
            classes.add(value.getClass()); // Dummy value, we don't need actual offset here
          }

          // Get the pickler for this concrete type
          @SuppressWarnings("unchecked") final PicklerInternal<Record> concretePickler =
              (PicklerInternal<Record>) picklerForRecord((Class<? extends Record>) value.getClass());

          // Add the size of the concrete object
          size += concretePickler.sizeOf((Record) value, classes);
          return size;
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
    ARRAY((byte) 13, 0, null),
    MAP((byte) 14, 0, Map.class),
    ENUM((byte) 15, 0, Enum.class);

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
      throw new IllegalArgumentException("Unknown type marker: " + marker);
    }
  }
}
