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
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

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


  /// Serializes an object into a byte buffer.
  ///
  /// @param object The object to serializeMany
  /// @param buffer The pre-allocated buffer to write to
  void serialize(T object, ByteBuffer buffer);

  /// Deserializes an object from a byte buffer.
  ///
  /// @param buffer The buffer to read from
  /// @return The deserialized object
  T deserialize(ByteBuffer buffer);

  /// Calculates the size in bytes required to serializeMany the given object
  ///
  /// This allows for pre-allocating buffers of the correct size without
  /// having to serializeMany the object first.
  ///
  /// @param value The object to calculate the size for
  /// @return The number of bytes required to serializeMany the object
  int sizeOf(T value);

  /// Get a Pickler for a record class, creating one if it doesn't exist in the registry
  @SuppressWarnings("unchecked")
  static <R extends Record> Pickler<R> picklerForRecord(Class<R> recordClass) {
    // Check if we already have a Pickler for this record class
    return (Pickler<R>) PICKLER_REGISTRY.computeIfAbsent(recordClass, clazz -> {
      if (Record.class.equals(clazz)) {
        throw new IllegalArgumentException("You have passed in Record.class as the type of your record. " +
            "This is the base class with no components so we cannot use it to reflect on your actual subclasses.");
      }
      // Check if the class is a record
      if (!clazz.isRecord()) {
        final var msg = "Class is not a record: " + clazz.getName();
        LOGGER.severe(() -> msg);
        throw new IllegalArgumentException(msg);
      }
      return PicklerBase.createPicklerForRecord((Class<R>) clazz);
    });
  }

  /// Get a Pickler for a record class, creating one if it doesn't exist in the registry
  /// The returned pickler creates record picklers for the permitted subclasses of the sealed interface
  @SuppressWarnings("unchecked")
  static <T> Pickler<T> picklerForSealedInterface(Class<T> sealedClass) {
    return (Pickler<T>) PICKLER_REGISTRY.computeIfAbsent(sealedClass, clazz -> PicklerBase.createPicklerForSealedInterface((Class<T>) clazz));
  }

  /// Serialize an array of records.
  /// Due to erasure it seems that sealed interfaces cannot confine the type of the array
  ///
  /// @param array The array of records to serializeMany
  /// @param buffer The buffer to write to
  /// @param <R> The record type
  static <R extends Record> void serializeMany(R[] array, ByteBuffer buffer) {
    // Get the pickler for the component type
    @SuppressWarnings("unchecked")
    Pickler<R> pickler = picklerForRecord((Class<R>) array.getClass().getComponentType());

    // Write array marker
    buffer.put(Constants.ARRAY.marker());

    // Write component type
    Map<Class<?>, Integer> class2BufferOffset = new HashMap<>();
    PicklerBase.writeClassNameWithDeduplication(buffer, array.getClass().getComponentType(), class2BufferOffset);

    // Write array length
    buffer.putInt(array.length);

    // Write each element
    for (R element : array) {
      pickler.serialize(element, buffer);
    }
  }

  /// Deserialize what was a record array into a list of records.
  ///
  /// @param <R>           The record type
  /// @param componentType The component type of the array
  /// @param buffer        The buffer to read from
  /// @return The deserialized objects in the order they were serialized
  static <R extends Record> List<R> deserializeMany(Class<R> componentType, ByteBuffer buffer) {
    // Skip the array marker if present
    if (buffer.get(buffer.position()) == Constants.ARRAY.marker()) {
      buffer.get(); // Consume the marker

      // Read component type
      Map<Integer, Class<?>> bufferOffset2Class = new HashMap<>();
      try {
        Class<?> readComponentType = PicklerBase.resolveClass(buffer, bufferOffset2Class);
        if (!componentType.equals(readComponentType)) {
          final var msg = "Component type mismatch: expected " + componentType.getName() +
              " but got " + readComponentType.getName();
          LOGGER.severe(() -> msg);
          throw new IllegalArgumentException(msg);
        }
      } catch (ClassNotFoundException e) {
        final var msg = "Failed to load component type class: " + e.getMessage();
        LOGGER.severe(() -> msg);
        throw new IllegalArgumentException(msg, e);
      }
    } else {
      // If no array marker, rewind to original position
      buffer.position(buffer.position() - 1);
    }

    // Get the pickler for the component type
    Pickler<R> pickler = picklerForRecord(componentType);

    return IntStream.range(0, buffer.getInt())
        .mapToObj(i -> pickler.deserialize(buffer))
        .toList();
  }

  /// This method is to work around restrictions in Java generics. It is not possible to say that something is a type
  /// that both extends a record and implements a sealed interface. This method has to just force the cast. Yet we can
  /// filter at runtime with `recordsOf` to ensure that we only have records. You will never get a runtime
  /// exception if you apply this method to a stream of records created through `recordsOf`:
  /// ```java
  /// int totalSize = recordsOf(stream).mapToInt(Pickler::sized).sum();
  ///```
  static <T extends Record> int sized(T value) {
    //noinspection unchecked
    return picklerForRecord((Class<T>) value.getClass()).sizeOf(value);
  }


  /// Calculate the size of an array of records
  ///
  /// @param array The array of records
  /// @param <R> The record type
  /// @return The size in bytes
  static <R extends Record> int sizeOfHomogeneousArray(R[] array) {
    if (array == null) {
      return 1; // Just the NULL marker
    }

    // This feels unnatural yet the final array can go into a lambda and hopefully escape analysis puts it onto the stack
    final var size = new int[]{1};

    // Add size for component type name (4 bytes for length + name bytes)
    final var componentTypeName = array.getClass().getComponentType().getName();
    size[0] += 4 + componentTypeName.getBytes(UTF_8).length;

    // Add 4 bytes for array length
    size[0] += 4;

    // Get the pickler for the component type
    @SuppressWarnings("unchecked") final var pickler = picklerForRecord((Class<R>) array.getClass().getComponentType());

    // Add size of each element using streams
    Arrays.stream(array)
        .mapToInt(pickler::sizeOf)
        .forEach(elementSize -> size[0] += elementSize);

    return size[0];
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

    /// Helper method to write a class name to a buffer with deduplication.
    /// If the class has been seen before, writes a negative reference instead of the full name.
    ///
    /// @param buffer The buffer to write to
    /// @param clazz The class to write
    /// @param class2BufferOffset Map tracking class to buffer position
    public static void writeClassNameWithDeduplication(ByteBuffer buffer, Class<?> clazz,
                                                       Map<Class<?>, Integer> class2BufferOffset) {
      // Check if we've seen this class before
      Integer existingOffset = class2BufferOffset.get(clazz);
      if (existingOffset != null) {
        // We've seen this class before, write a negative reference
        int reference = ~existingOffset;
        buffer.putInt(reference); // Using bitwise complement for negative reference
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
      }
    }

    /// Helper method to read a class name from a buffer with deduplication support.
    ///
    /// @param buffer The buffer to read from
    /// @param bufferOffset2Class Map tracking buffer position to class
    /// @return The loaded class
    public static Class<?> resolveClass(ByteBuffer buffer,
                                        Map<Integer, Class<?>> bufferOffset2Class)
        throws ClassNotFoundException {

      // Read the class name length or reference
      int componentTypeLength = buffer.getInt();

      if (componentTypeLength > Short.MAX_VALUE) {
        final var msg = "The max length of a string in java is 65535 bytes, " +
            "but the length of the class name is " + componentTypeLength;
        LOGGER.severe(() -> msg);
        throw new IllegalArgumentException(msg);
      }

      if (componentTypeLength < 0) {
        // This is a reference to a previously seen class
        int offset = ~componentTypeLength; // Decode the reference using bitwise complement
        Class<?> referencedClass = bufferOffset2Class.get(offset);

        if (referencedClass == null) {
          final var msg = "Invalid class reference offset: " + offset;
          LOGGER.severe(() -> msg);
          throw new IllegalArgumentException(msg);
        }
        return referencedClass;
      } else {
        // This is a new class name
        int currentPosition = buffer.position() - 4; // Position before reading the length

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
      // Write the number of components as an unsigned byte (max 255)
      writeUnsignedByte(buffer, (short) components.length);
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
      if (c.getClass().isArray()) {
        final int[] arrayHeaderSize = {4 + 4}; // 4 bytes for length + 4 bytes for type name length - use array for mutability
        if (!classes.contains(c.getClass())) {
          // Component type name size
          String componentTypeName = c.getClass().getComponentType().getName();
          byte[] componentTypeBytes = componentTypeName.getBytes(UTF_8);
          arrayHeaderSize[0] += componentTypeBytes.length;
          classes.add(c.getClass());
        }

        // Calculate size of all array elements
        int length = Array.getLength(c);
        final int[] elementsSize = {0};

        for (int i = 0; i < length; i++) {
          final Object element = Array.get(c, i);
          final int elementSize = staticSizeOf(element, classes);
          elementsSize[0] += elementSize;
        }

        size += arrayHeaderSize[0] + elementsSize[0];
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
          classes.add(c.getClass());
        }

        // Add size for enum constant name
        String enumConstantName = enumValue.name();
        byte[] enumNameBytes = enumConstantName.getBytes(UTF_8);
        int constantNameSize = 4 + enumNameBytes.length; // 4 bytes for length + name bytes
        size += constantNameSize;
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
        case List<?> ignored -> LIST.marker();
        default -> throw new IllegalArgumentException("Unsupported type: " + c.getClass());
      };
    }

    static void write(Map<Class<?>, Integer> class2BufferOffset, ByteBuffer buffer, Object c) {
      if (c == null) {
        buffer.put(NULL.marker());
        return;
      }

      if (c.getClass().isArray()) {
        buffer.put(ARRAY.marker());

        writeClassNameWithDeduplication(buffer, c.getClass().getComponentType(), class2BufferOffset);

        // Write the array length
        int length = Array.getLength(c);
        buffer.putInt(length);

        if (byte.class.equals(c.getClass().getComponentType())) {
          buffer.put((byte[]) c);
        } else {
          IntStream.range(0, length).forEach(i -> write(class2BufferOffset, buffer, Array.get(c, i)));
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

          // Use that pickler to serializeMany the nested record
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
        case List<?> list -> {
          buffer.put(typeMarker(c));

          // Write the number of elements
          buffer.putInt(list.size());

          // Write each element
          list.forEach(element -> write(class2BufferOffset, buffer, element));
        }
        case Enum<?> enumValue -> {
          buffer.put(typeMarker(c));

          // Write the enum class name with deduplication
          writeClassNameWithDeduplication(buffer, enumValue.getClass(), class2BufferOffset);

          // Write the enum constant name
          String enumConstantName = enumValue.name();
          byte[] enumNameBytes = enumConstantName.getBytes(UTF_8);

          buffer.putInt(enumNameBytes.length);
          buffer.put(enumNameBytes);
        }
        default -> throw new IllegalArgumentException("Unsupported type: " + c.getClass());
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
      // Read the number of components as an unsigned byte
      final short length = readUnsignedByte(buffer);
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
            PicklerInternal<Record> nestedPickler = (PicklerInternal<Record>) Pickler.picklerForRecord((Class<? extends Record>) recordClass);

            // Deserialize the nested record
            yield nestedPickler.deserialize(buffer, bufferOffset2Class);
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
                        deserializeValue(bufferOffset2Class, buffer),
                        deserializeValue(bufferOffset2Class, buffer)))
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
              final var msg = "Failed to access component: " + i +
                  " in record class '" + recordClassName + "' : " + e.getMessage();
              LOGGER.severe(() -> msg);
              throw new IllegalArgumentException(msg, e);
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
    /// 5. Uses the specific pickler to serializeMany the object
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
    static <T> Pickler<?> createPicklerForSealedInterface(Class<T> sealedClass) {
      @SuppressWarnings("unchecked") Pickler<T> existingPickler = (Pickler<T>) PICKLER_REGISTRY.get(sealedClass);
      if (existingPickler != null) {
        return existingPickler;
      }

      // Get all permitted record subclasses, recursively traversing sealed interfaces
      Class<?>[] subclasses = allPermittedRecordClasses(sealedClass).toArray(Class<?>[]::new);

      // Force the pre-creation and caching of picklers for all permitted record of all nested permitted sealed interfaces
      //noinspection unchecked
      Arrays.stream(subclasses).filter(Record.class::isAssignableFrom).forEach(permitted -> createPicklerForRecord((Class<? extends Record>) permitted));

      // build a map of the names of the permitted record subclasses to their class objects
      @SuppressWarnings("unchecked") final Map<String, Class<? extends Record>> classNamesToPermittedRecords = Arrays.stream(subclasses)
          .filter(Record.class::isAssignableFrom)
          .map(permitted -> (Class<? extends Record>) permitted)
          .map(permitted -> Map.entry(permitted.getName(), permitted))
          .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

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

          // Use that pickler to serializeMany the object
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

          String className;
          // Read the class name length or reference
          int componentTypeLength = buffer.getInt();

          if (componentTypeLength > Short.MAX_VALUE) {
            final var msg = "The max length of a string in java is 65535 bytes, " +
                "but the length of the class name is " + componentTypeLength;
            LOGGER.severe(() -> msg);
            throw new IllegalArgumentException(msg);
          }

          if (componentTypeLength < 0) {
            // This is a reference to a previously seen class
            int offset = ~componentTypeLength; // Decode the reference using bitwise complement
            Class<?> referencedClass = bufferOffset2Class.get(offset);

            if (referencedClass == null) {
              final var msg = "Invalid class reference offset: " + offset;
              LOGGER.severe(() -> msg);
              throw new IllegalArgumentException(msg);
            }
            className = referencedClass.getName();
          } else {
            // This is a new class name
            int currentPosition = buffer.position() - 4; // Position before reading the length

            if (buffer.remaining() < componentTypeLength) {
              final var msg = "Buffer underflow: needed " + componentTypeLength +
                  " bytes but only " + buffer.remaining() + " remaining";
              LOGGER.severe(() -> msg);
              throw new IllegalArgumentException(msg);
            }

            // Read the class name
            byte[] classNameBytes = new byte[componentTypeLength];
            buffer.get(classNameBytes);
            className = new String(classNameBytes, UTF_8);

            // Validate class name - add basic validation that allows array type names like `[I`, `[[I`, `[L`java.lang.String;` etc.
            if (!className.matches("[\\[\\]a-zA-Z0-9_.$;]+")) {
              final var msg = "Invalid class name format: " + className;
              LOGGER.severe(() -> msg);
              throw new IllegalArgumentException(msg);
            }

            final var classType = classNamesToPermittedRecords.get(className);

            // Store the position where we wrote this class
            bufferOffset2Class.put(currentPosition, classType);
          }

          final var permittedClass = classNamesToPermittedRecords.get(className);

          if (permittedClass == null) {
            final var msg = "No permitted subclass found for class '" + className + "' permitted subclasses: " +
                String.join(",", classNamesToPermittedRecords.keySet());
            LOGGER.severe(() -> msg);
            throw new IllegalArgumentException(msg);
          }

          // Get the pickler for this concrete class
          @SuppressWarnings("unchecked") PicklerInternal<Record> concretePickler =
              (PicklerInternal<Record>) Pickler.picklerForRecord(classNamesToPermittedRecords.get(className));

          if (concretePickler == null) {
            final var msg = "No permitted subclass pickler found for class '" + className + "' permitted subclasses: " +
                String.join(",", classNamesToPermittedRecords.keySet());
            LOGGER.severe(() -> msg);
            throw new IllegalArgumentException(msg);
          }

          String finalClassName1 = className;
          LOGGER.fine(() -> "Deserializing " + finalClassName1 +
              " with pickler: " + concretePickler.hashCode());

          // Deserialize using the concrete pickler
          @SuppressWarnings("unchecked") T result = (T) concretePickler.deserialize(buffer, bufferOffset2Class);

          return result;
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
}
