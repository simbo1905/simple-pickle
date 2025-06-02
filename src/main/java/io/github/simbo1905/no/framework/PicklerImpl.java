// SPDX-FileCopyrightText: 2025 Simon Massey
// SPDX-License-Identifier: MIT

package io.github.simbo1905.no.framework;

import java.lang.invoke.MethodHandle;
import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.stream.IntStream;

/// Unified pickler implementation that handles all reachable types using array-based architecture.
/// Eliminates the need for separate RecordPickler and SealedPickler classes.
class PicklerImpl<T> implements Pickler<T> {

  private final Machinery2.UnifiedTypeAnalysis analysis;

  /// Create a unified pickler for any root type (record, enum, or sealed interface)
  PicklerImpl(Class<T> rootClass) {
    Objects.requireNonNull(rootClass, "rootClass cannot be null");

    LOGGER.info(() -> "Creating unified pickler for root class: " + rootClass.getName());

    // Perform exhaustive type analysis
    this.analysis = Machinery2.analyzeAllReachableTypes(rootClass);

    LOGGER.info(() -> "Unified pickler created for " + rootClass.getSimpleName() +
        " with " + analysis.discoveredClasses().length + " total reachable types");
  }

  @Override
  public int serialize(WriteBuffer buffer, T object) {
    final var writeBufferImpl = (WriteBufferImpl) buffer;
    final var startPosition = writeBufferImpl.position();
    LOGGER.finer(() -> "PicklerImpl.serialize: startPosition=" + startPosition + " object=" + object);

    if (object == null) {
      ZigZagEncoding.putInt(writeBufferImpl.buffer, 0); // NULL marker
      LOGGER.finer(() -> "PicklerImpl.serialize: wrote NULL marker, bytes=" + (writeBufferImpl.position() - startPosition));
      return writeBufferImpl.position() - startPosition;
    }

    // Get concrete runtime type and its ordinal
    @SuppressWarnings("unchecked")
    Class<? extends T> concreteType = (Class<? extends T>) object.getClass();
    LOGGER.finer(() -> "PicklerImpl.serialize: concreteType=" + concreteType.getSimpleName());

    int physicalIndex = analysis.getOrdinal(concreteType);
    int logicalOrdinal = physicalIndex + 1; // Convert: physical array index 0 -> logical ordinal 1
    LOGGER.finer(() -> "PicklerImpl.serialize: physicalIndex=" + physicalIndex + " logicalOrdinal=" + logicalOrdinal);

    // Write 1-indexed logical ordinal for user types (1, 2, 3, ...)
    ZigZagEncoding.putInt(writeBufferImpl.buffer, logicalOrdinal);
    LOGGER.finer(() -> "PicklerImpl.serialize: wrote logicalOrdinal=" + logicalOrdinal + " position=" + writeBufferImpl.position());

    // Serialize based on tag (using physical array index)
    Tag tag = analysis.tags()[physicalIndex];
    LOGGER.finer(() -> "PicklerImpl.serialize: tag=" + tag + " for physicalIndex=" + physicalIndex + " object=" + object);

    // Handle serialization by tag type
    switch (tag) {
      case ENUM -> {
        // For enums, write the constant name after the logical ordinal
        LOGGER.finer(() -> "PicklerImpl.serialize: serializing enum " + object);
        Enum<?> enumValue = (Enum<?>) object;
        String constantName = enumValue.name();
        byte[] nameBytes = constantName.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        ZigZagEncoding.putInt(writeBufferImpl.buffer, nameBytes.length);
        writeBufferImpl.buffer.put(nameBytes);
        LOGGER.finer(() -> "PicklerImpl.serialize: wrote enum constant=" + constantName + " length=" + nameBytes.length);
      }
      case RECORD -> {
        // For records, serialize all components
        LOGGER.finer(() -> "PicklerImpl.serialize: serializing record components for " + object);
        serializeRecordComponents(writeBufferImpl, concreteType, object, physicalIndex);
      }
      default -> throw new IllegalStateException("Unsupported tag for user type: " + tag);
    }

    final var totalBytes = writeBufferImpl.position() - startPosition;
    LOGGER.finer(() -> "PicklerImpl.serialize: completed, totalBytes=" + totalBytes);
    return totalBytes;
  }

  /// Serialize all components of a record using method handles and built-in type writers
  private void serializeRecordComponents(WriteBufferImpl writeBufferImpl, Class<?> recordClass, Object record, int ordinal) {
    LOGGER.finer(() -> "serializeRecordComponents: starting for " + recordClass.getSimpleName() + " ordinal=" + ordinal + " recordClass=" + recordClass.getName() + " actualRecord=" + record.getClass().getName());

    // Get component accessors for this record type from analysis
    var componentAccessors = analysis.componentAccessors()[ordinal];
    if (componentAccessors == null) {
      LOGGER.finer(() -> "serializeRecordComponents: no components for " + recordClass.getSimpleName());
      return;
    }

    LOGGER.finer(() -> "serializeRecordComponents: found " + componentAccessors.length + " components");

    // Serialize each component using its accessor
    for (int i = 0; i < componentAccessors.length; i++) {
      final int componentIndex = i; // Make final for lambda capture
      try {
        var accessor = componentAccessors[i];
        Object componentValue = accessor.invoke(record);
        LOGGER.finer(() -> "serializeRecordComponents: component[" + componentIndex + "] value=" + componentValue + " type=" + (componentValue != null ? componentValue.getClass().getSimpleName() : "null") + " className=" + (componentValue != null ? componentValue.getClass().getName() : "null"));

        // Check if this is a user type or built-in type
        if (componentValue != null && isUserType(componentValue.getClass())) {
          // Recursively serialize user types using their ordinals
          LOGGER.finer(() -> "serializeRecordComponents: found user type " + componentValue.getClass().getSimpleName());
          serializeUserType(writeBufferImpl, componentValue);
        } else {
          // Handle built-in types directly
          serializeBuiltInComponent(writeBufferImpl, componentValue);
        }

      } catch (Throwable e) {
        throw new RuntimeException("Failed to serialize component " + componentIndex + " of " + recordClass.getSimpleName(), e);
      }
    }

    LOGGER.finer(() -> "serializeRecordComponents: completed for " + recordClass.getSimpleName());
  }

  /// Serialize a component value that is a built-in type (String, int, etc.)
  private void serializeBuiltInComponent(WriteBufferImpl writeBufferImpl, Object componentValue) {
    LOGGER.finer(() -> "serializeBuiltInComponent: value=" + componentValue + " type=" + (componentValue != null ? componentValue.getClass().getSimpleName() : "null"));

    if (componentValue == null) {
      ZigZagEncoding.putInt(writeBufferImpl.buffer, 0); // NULL marker
      LOGGER.finer(() -> "serializeBuiltInComponent: wrote NULL marker");
      return;
    }

    // Handle built-in types with negative markers (using Constants mapping)
    switch (componentValue) {
      case String str -> {
        ZigZagEncoding.putInt(writeBufferImpl.buffer, Constants.STRING.wireMarker());
        byte[] bytes = str.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        ZigZagEncoding.putInt(writeBufferImpl.buffer, bytes.length);
        writeBufferImpl.buffer.put(bytes);
        LOGGER.finer(() -> "serializeBuiltInComponent: wrote String length=" + bytes.length);
      }
      case Integer intValue -> {
        ZigZagEncoding.putInt(writeBufferImpl.buffer, Constants.INTEGER.wireMarker());
        ZigZagEncoding.putInt(writeBufferImpl.buffer, intValue);
        LOGGER.finer(() -> "serializeBuiltInComponent: wrote Integer=" + intValue);
      }
      case Boolean boolValue -> {
        ZigZagEncoding.putInt(writeBufferImpl.buffer, Constants.BOOLEAN.wireMarker());
        writeBufferImpl.buffer.put((byte) (boolValue ? 1 : 0));
        LOGGER.finer(() -> "serializeBuiltInComponent: wrote Boolean=" + boolValue);
      }
      case Long longValue -> {
        ZigZagEncoding.putInt(writeBufferImpl.buffer, Constants.LONG.wireMarker());
        ZigZagEncoding.putLong(writeBufferImpl.buffer, longValue);
        LOGGER.finer(() -> "serializeBuiltInComponent: wrote Long=" + longValue);
      }
      case Float floatValue -> {
        ZigZagEncoding.putInt(writeBufferImpl.buffer, Constants.FLOAT.wireMarker());
        writeBufferImpl.buffer.putFloat(floatValue);
        LOGGER.finer(() -> "serializeBuiltInComponent: wrote Float=" + floatValue);
      }
      case Double doubleValue -> {
        ZigZagEncoding.putInt(writeBufferImpl.buffer, Constants.DOUBLE.wireMarker());
        writeBufferImpl.buffer.putDouble(doubleValue);
        LOGGER.finer(() -> "serializeBuiltInComponent: wrote Double=" + doubleValue);
      }
      case Byte byteValue -> {
        ZigZagEncoding.putInt(writeBufferImpl.buffer, Constants.BYTE.wireMarker());
        writeBufferImpl.buffer.put(byteValue);
        LOGGER.finer(() -> "serializeBuiltInComponent: wrote Byte=" + byteValue);
      }
      case Short shortValue -> {
        ZigZagEncoding.putInt(writeBufferImpl.buffer, Constants.SHORT.wireMarker());
        writeBufferImpl.buffer.putShort(shortValue);
        LOGGER.finer(() -> "serializeBuiltInComponent: wrote Short=" + shortValue);
      }
      case Character charValue -> {
        ZigZagEncoding.putInt(writeBufferImpl.buffer, Constants.CHARACTER.wireMarker());
        writeBufferImpl.buffer.putChar(charValue);
        LOGGER.finer(() -> "serializeBuiltInComponent: wrote Character=" + charValue);
      }
      case java.util.UUID uuidValue -> {
        ZigZagEncoding.putInt(writeBufferImpl.buffer, Constants.UUID.wireMarker());
        writeBufferImpl.buffer.putLong(uuidValue.getMostSignificantBits());
        writeBufferImpl.buffer.putLong(uuidValue.getLeastSignificantBits());
        LOGGER.finer(() -> "serializeBuiltInComponent: wrote UUID=" + uuidValue);
      }
      case java.util.Optional<?> optional -> {
        if (optional.isEmpty()) {
          ZigZagEncoding.putInt(writeBufferImpl.buffer, Constants.OPTIONAL_EMPTY.wireMarker());
          LOGGER.finer(() -> "serializeBuiltInComponent: wrote Optional.empty()");
        } else {
          ZigZagEncoding.putInt(writeBufferImpl.buffer, Constants.OPTIONAL_OF.wireMarker());
          // Recursively serialize the value inside the Optional
          serializeBuiltInComponent(writeBufferImpl, optional.get());
          LOGGER.finer(() -> "serializeBuiltInComponent: wrote Optional.of(" + optional.get() + ")");
        }
      }
      case java.util.List<?> list -> {
        ZigZagEncoding.putInt(writeBufferImpl.buffer, Constants.LIST.wireMarker());
        ZigZagEncoding.putInt(writeBufferImpl.buffer, list.size());
        LOGGER.finer(() -> "serializeBuiltInComponent: wrote List size=" + list.size());
        for (Object item : list) {
          serializeBuiltInComponent(writeBufferImpl, item);
        }
        LOGGER.finer(() -> "serializeBuiltInComponent: completed List serialization");
      }
      case java.util.Map<?, ?> map -> {
        ZigZagEncoding.putInt(writeBufferImpl.buffer, Constants.MAP.wireMarker());
        ZigZagEncoding.putInt(writeBufferImpl.buffer, map.size());
        LOGGER.finer(() -> "serializeBuiltInComponent: wrote Map size=" + map.size());
        for (var entry : map.entrySet()) {
          serializeBuiltInComponent(writeBufferImpl, entry.getKey());
          serializeBuiltInComponent(writeBufferImpl, entry.getValue());
        }
        LOGGER.finer(() -> "serializeBuiltInComponent: completed Map serialization");
      }
      default -> {
        // Check if it's an array
        if (componentValue.getClass().isArray()) {
          ZigZagEncoding.putInt(writeBufferImpl.buffer, Constants.ARRAY.wireMarker());
          serializeArray(writeBufferImpl, componentValue);
        } else {
          throw new IllegalArgumentException("Unsupported built-in component type: " + componentValue.getClass());
        }
      }
    }
  }

  /// Serialize an array of any type (primitive or object arrays) - ported from working Machinary.java
  private void serializeArray(WriteBufferImpl writeBufferImpl, Object arrayValue) {
    LOGGER.finer(() -> "serializeArray: value=" + arrayValue + " class=" + arrayValue.getClass().getSimpleName());

    switch (arrayValue) {
      case byte[] arr -> {
        writeBufferImpl.buffer.put(Constants.BYTE.marker());
        ZigZagEncoding.putInt(writeBufferImpl.buffer, arr.length);
        writeBufferImpl.buffer.put(arr);
        LOGGER.finer(() -> "serializeArray: byte[] length=" + arr.length);
      }
      case boolean[] booleans -> {
        writeBufferImpl.buffer.put(Constants.BOOLEAN.marker());
        int length = booleans.length;
        ZigZagEncoding.putInt(writeBufferImpl.buffer, length);
        java.util.BitSet bitSet = new java.util.BitSet(length);
        java.util.stream.IntStream.range(0, length)
            .filter(i -> booleans[i])
            .forEach(bitSet::set);
        byte[] bytes = bitSet.toByteArray();
        ZigZagEncoding.putInt(writeBufferImpl.buffer, bytes.length);
        writeBufferImpl.buffer.put(bytes);
        LOGGER.finer(() -> "serializeArray: boolean[] length=" + length);
      }
      case short[] shorts -> {
        writeBufferImpl.buffer.put(Constants.SHORT.marker());
        ZigZagEncoding.putInt(writeBufferImpl.buffer, shorts.length);
        for (short s : shorts) {
          writeBufferImpl.buffer.putShort(s);
        }
        LOGGER.finer(() -> "serializeArray: short[] length=" + shorts.length);
      }
      case int[] integers -> {
        writeBufferImpl.buffer.put(Constants.INTEGER.marker());
        ZigZagEncoding.putInt(writeBufferImpl.buffer, integers.length);
        for (int i : integers) {
          writeBufferImpl.buffer.putInt(i);
        }
        LOGGER.finer(() -> "serializeArray: int[] length=" + integers.length);
      }
      case long[] longs -> {
        writeBufferImpl.buffer.put(Constants.LONG.marker());
        ZigZagEncoding.putInt(writeBufferImpl.buffer, longs.length);
        for (long l : longs) {
          writeBufferImpl.buffer.putLong(l);
        }
        LOGGER.finer(() -> "serializeArray: long[] length=" + longs.length);
      }
      case float[] floats -> {
        writeBufferImpl.buffer.put(Constants.FLOAT.marker());
        ZigZagEncoding.putInt(writeBufferImpl.buffer, floats.length);
        for (float f : floats) {
          writeBufferImpl.buffer.putFloat(f);
        }
        LOGGER.finer(() -> "serializeArray: float[] length=" + floats.length);
      }
      case double[] doubles -> {
        writeBufferImpl.buffer.put(Constants.DOUBLE.marker());
        ZigZagEncoding.putInt(writeBufferImpl.buffer, doubles.length);
        for (double d : doubles) {
          writeBufferImpl.buffer.putDouble(d);
        }
        LOGGER.finer(() -> "serializeArray: double[] length=" + doubles.length);
      }
      case char[] chars -> {
        writeBufferImpl.buffer.put(Constants.CHARACTER.marker());
        ZigZagEncoding.putInt(writeBufferImpl.buffer, chars.length);
        for (char c : chars) {
          writeBufferImpl.buffer.putChar(c);
        }
        LOGGER.finer(() -> "serializeArray: char[] length=" + chars.length);
      }
      case String[] strings -> {
        writeBufferImpl.buffer.put(Constants.STRING.marker());
        ZigZagEncoding.putInt(writeBufferImpl.buffer, strings.length);
        for (String s : strings) {
          byte[] bytes = s.getBytes(java.nio.charset.StandardCharsets.UTF_8);
          ZigZagEncoding.putInt(writeBufferImpl.buffer, bytes.length);
          writeBufferImpl.buffer.put(bytes);
        }
        LOGGER.finer(() -> "serializeArray: String[] length=" + strings.length);
      }
      default -> {
        // Generic object array - handle Record[] and Optional[] arrays
        Class<?> componentType = arrayValue.getClass().getComponentType();

        if (componentType == java.util.Optional.class) {
          // Handle Optional[] arrays
          java.util.Optional<?>[] optionals = (java.util.Optional<?>[]) arrayValue;
          writeBufferImpl.buffer.put(Constants.OPTIONAL_OF.marker());
          ZigZagEncoding.putInt(writeBufferImpl.buffer, optionals.length);
          for (java.util.Optional<?> opt : optionals) {
            if (opt.isEmpty()) {
              writeBufferImpl.buffer.put(Constants.OPTIONAL_EMPTY.marker());
            } else {
              writeBufferImpl.buffer.put(Constants.OPTIONAL_OF.marker());
              serializeBuiltInComponent(writeBufferImpl, opt.get());
            }
          }
          LOGGER.finer(() -> "serializeArray: Optional[] length=" + optionals.length);
        } else {
          // Handle Record[] arrays and other object arrays using ordinals (no class names)
          Object[] objectArray = (Object[]) arrayValue;
          writeBufferImpl.buffer.put(Constants.RECORD.marker()); // Use RECORD marker for object arrays
          ZigZagEncoding.putInt(writeBufferImpl.buffer, objectArray.length);

          // NO class name serialization in unified pickler - use ordinals only
          for (Object item : objectArray) {
            if (item == null) {
              ZigZagEncoding.putInt(writeBufferImpl.buffer, 0); // NULL marker
            } else if (isUserType(item.getClass())) {
              serializeUserType(writeBufferImpl, item);
            } else {
              serializeBuiltInComponent(writeBufferImpl, item);
            }
          }
          LOGGER.finer(() -> "serializeArray: " + componentType.getSimpleName() + "[] length=" + objectArray.length + " (ordinal-based)");
        }
      }
    }
  }

  /// Check if a class is a user type (discovered by analysis) vs built-in type
  private boolean isUserType(Class<?> clazz) {
    try {
      analysis.getOrdinal(clazz);
      return true; // Found in discovered user types
    } catch (IllegalArgumentException e) {
      return false; // Not a discovered user type, must be built-in
    }
  }

  /// Serialize a user type (record/enum) using its ordinal from analysis
  private void serializeUserType(WriteBufferImpl writeBufferImpl, Object userTypeValue) {
    LOGGER.finer(() -> "serializeUserType: value=" + userTypeValue + " type=" + userTypeValue.getClass().getSimpleName() + " className=" + userTypeValue.getClass().getName());

    Class<?> userClass = userTypeValue.getClass();
    int physicalIndex = analysis.getOrdinal(userClass);
    int logicalOrdinal = physicalIndex + 1; // Convert: physical array index 0 -> logical ordinal 1
    Tag tag = analysis.tags()[physicalIndex];

    LOGGER.finer(() -> "serializeUserType: physicalIndex=" + physicalIndex + " logicalOrdinal=" + logicalOrdinal + " tag=" + tag + " userClass=" + userClass.getName());

    // Write the logical ordinal for this user type
    ZigZagEncoding.putInt(writeBufferImpl.buffer, logicalOrdinal);

    // Handle based on tag type
    switch (tag) {
      case ENUM -> {
        // For enums, serialize the constant name
        Enum<?> enumValue = (Enum<?>) userTypeValue;
        String constantName = enumValue.name();
        byte[] nameBytes = constantName.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        ZigZagEncoding.putInt(writeBufferImpl.buffer, nameBytes.length);
        writeBufferImpl.buffer.put(nameBytes);
        LOGGER.finer(() -> "serializeUserType: wrote enum constant=" + constantName);
      }
      case RECORD -> {
        // For records, recursively serialize components
        LOGGER.finer(() -> "serializeUserType: recursively serializing record components");
        serializeRecordComponents(writeBufferImpl, userClass, userTypeValue, physicalIndex);
      }
      default -> throw new IllegalStateException("Unsupported user type tag: " + tag);
    }
  }

  /// Deserialize a user type (record/enum) using its ordinal and tag information
  @SuppressWarnings("unchecked")
  private T deserializeUserType(ReadBufferImpl readBufferImpl, Class<?> userClass, Tag tag, int ordinal) {
    LOGGER.finer(() -> "deserializeUserType: class=" + userClass.getSimpleName() + " className=" + userClass.getName() + " tag=" + tag + " ordinal=" + ordinal);

    return switch (tag) {
      case ENUM -> {
        // For enums, read the constant name and look it up
        int nameLength = ZigZagEncoding.getInt(readBufferImpl.buffer);
        byte[] nameBytes = new byte[nameLength];
        readBufferImpl.buffer.get(nameBytes);
        String constantName = new String(nameBytes, java.nio.charset.StandardCharsets.UTF_8);
        LOGGER.finer(() -> "deserializeUserType: enum constant=" + constantName);

        @SuppressWarnings("rawtypes")
        Enum enumValue = Enum.valueOf((Class<Enum>) userClass, constantName);
        yield (T) enumValue;
      }
      case RECORD -> {
        // For records, deserialize components and invoke constructor
        LOGGER.finer(() -> "deserializeUserType: deserializing record components");
        Object[] componentValues = deserializeRecordComponents(readBufferImpl, ordinal);

        try {
          MethodHandle constructor = analysis.constructors()[ordinal];
          Object recordInstance = constructor.invokeWithArguments(componentValues);
          LOGGER.finer(() -> "deserializeUserType: created record instance=" + recordInstance);
          yield (T) recordInstance;
        } catch (Throwable e) {
          throw new RuntimeException("Failed to construct " + userClass.getSimpleName(), e);
        }
      }
      default -> throw new IllegalStateException("Unsupported user type tag: " + tag);
    };
  }

  /// Deserialize all components of a record using method handles and built-in type readers
  private Object[] deserializeRecordComponents(ReadBufferImpl readBufferImpl, int ordinal) {
    LOGGER.finer(() -> "deserializeRecordComponents: starting for ordinal=" + ordinal);

    // Get component accessors to determine how many components to read
    var componentAccessors = analysis.componentAccessors()[ordinal];
    if (componentAccessors == null) {
      LOGGER.finer(() -> "deserializeRecordComponents: no components");
      return new Object[0];
    }

    LOGGER.finer(() -> "deserializeRecordComponents: reading " + componentAccessors.length + " components");

    // Deserialize each component using functional approach
    Object[] componentValues = IntStream.range(0, componentAccessors.length)
        .mapToObj(componentIndex -> {
          // Read the marker/ordinal for this component
          int marker = ZigZagEncoding.getInt(readBufferImpl.buffer);
          LOGGER.finer(() -> "deserializeRecordComponents: component[" + componentIndex + "] marker=" + marker);

          if (marker == 0) {
            LOGGER.finer(() -> "deserializeRecordComponents: component[" + componentIndex + "] = null");
            return null;
          } else if (marker < 0) {
            // Built-in type with negative marker
            Object value = deserializeBuiltInComponent(readBufferImpl, -marker);
            LOGGER.finer(() -> "deserializeRecordComponents: component[" + componentIndex + "] = " + value + " (built-in)");
            return value;
          } else {
            // User type with positive logical ordinal, convert to physical array index
            int physicalIndex = marker - 1; // Convert: logical ordinal 1 -> physical array index 0
            Class<?> componentClass = analysis.discoveredClasses()[physicalIndex];
            Tag componentTag = analysis.tags()[physicalIndex];
            Object value = deserializeUserType(readBufferImpl, componentClass, componentTag, physicalIndex);
            LOGGER.finer(() -> "deserializeRecordComponents: component[" + componentIndex + "] = " + value + " (user type)");
            return value;
          }
        })
        .toArray();

    LOGGER.finer(() -> "deserializeRecordComponents: completed with " + componentValues.length + " values");
    return componentValues;
  }

  /// Deserialize a component value that is a built-in type using negative marker
  private Object deserializeBuiltInComponent(ReadBufferImpl readBufferImpl, int positiveMarker) {
    LOGGER.finer(() -> "deserializeBuiltInComponent: positiveMarker=" + positiveMarker);

    if (positiveMarker == Constants.BOOLEAN.marker()) {
      boolean value = readBufferImpl.buffer.get() != 0;
      LOGGER.finer(() -> "deserializeBuiltInComponent: read Boolean=" + value);
      return value;
    } else if (positiveMarker == Constants.INTEGER.marker()) {
      int value = ZigZagEncoding.getInt(readBufferImpl.buffer);
      LOGGER.finer(() -> "deserializeBuiltInComponent: read Integer=" + value);
      return value;
    } else if (positiveMarker == Constants.LONG.marker()) {
      long value = ZigZagEncoding.getLong(readBufferImpl.buffer);
      LOGGER.finer(() -> "deserializeBuiltInComponent: read Long=" + value);
      return value;
    } else if (positiveMarker == Constants.FLOAT.marker()) {
      float value = readBufferImpl.buffer.getFloat();
      LOGGER.finer(() -> "deserializeBuiltInComponent: read Float=" + value);
      return value;
    } else if (positiveMarker == Constants.DOUBLE.marker()) {
      double value = readBufferImpl.buffer.getDouble();
      LOGGER.finer(() -> "deserializeBuiltInComponent: read Double=" + value);
      return value;
    } else if (positiveMarker == Constants.BYTE.marker()) {
      byte value = readBufferImpl.buffer.get();
      LOGGER.finer(() -> "deserializeBuiltInComponent: read Byte=" + value);
      return value;
    } else if (positiveMarker == Constants.SHORT.marker()) {
      short value = readBufferImpl.buffer.getShort();
      LOGGER.finer(() -> "deserializeBuiltInComponent: read Short=" + value);
      return value;
    } else if (positiveMarker == Constants.CHARACTER.marker()) {
      char value = readBufferImpl.buffer.getChar();
      LOGGER.finer(() -> "deserializeBuiltInComponent: read Character=" + value);
      return value;
    } else if (positiveMarker == Constants.UUID.marker()) {
      long mostSigBits = readBufferImpl.buffer.getLong();
      long leastSigBits = readBufferImpl.buffer.getLong();
      java.util.UUID value = new java.util.UUID(mostSigBits, leastSigBits);
      LOGGER.finer(() -> "deserializeBuiltInComponent: read UUID=" + value);
      return value;
    } else if (positiveMarker == Constants.STRING.marker()) {
      int length = ZigZagEncoding.getInt(readBufferImpl.buffer);
      byte[] bytes = new byte[length];
      readBufferImpl.buffer.get(bytes);
      String value = new String(bytes, java.nio.charset.StandardCharsets.UTF_8);
      LOGGER.finer(() -> "deserializeBuiltInComponent: read String length=" + length + " value=" + value);
      return value;
    } else if (positiveMarker == Constants.OPTIONAL_OF.marker()) {
      Object innerValue = deserializeBuiltInComponent(readBufferImpl, -ZigZagEncoding.getInt(readBufferImpl.buffer));
      java.util.Optional<Object> value = java.util.Optional.of(innerValue);
      LOGGER.finer(() -> "deserializeBuiltInComponent: read Optional.of(" + innerValue + ")");
      return value;
    } else if (positiveMarker == Constants.OPTIONAL_EMPTY.marker()) {
      LOGGER.finer(() -> "deserializeBuiltInComponent: read Optional.empty()");
      return java.util.Optional.empty();
    } else if (positiveMarker == Constants.LIST.marker()) {
      int size = ZigZagEncoding.getInt(readBufferImpl.buffer);
      LOGGER.finer(() -> "deserializeBuiltInComponent: reading List size=" + size);
      java.util.List<Object> list = IntStream.range(0, size)
          .mapToObj(i -> {
            int itemMarker = ZigZagEncoding.getInt(readBufferImpl.buffer);
            return itemMarker == 0 ? null : deserializeBuiltInComponent(readBufferImpl, -itemMarker);
          })
          .collect(java.util.stream.Collectors.toList());
      LOGGER.finer(() -> "deserializeBuiltInComponent: completed List=" + list);
      return list;
    } else if (positiveMarker == Constants.MAP.marker()) {
      int size = ZigZagEncoding.getInt(readBufferImpl.buffer);
      LOGGER.finer(() -> "deserializeBuiltInComponent: reading Map size=" + size);
      java.util.Map<Object, Object> map = IntStream.range(0, size)
          .boxed()
          .collect(java.util.stream.Collectors.toMap(
              i -> {
                int keyMarker = ZigZagEncoding.getInt(readBufferImpl.buffer);
                return keyMarker == 0 ? null : deserializeBuiltInComponent(readBufferImpl, -keyMarker);
              },
              i -> {
                int valueMarker = ZigZagEncoding.getInt(readBufferImpl.buffer);
                return valueMarker == 0 ? null : deserializeBuiltInComponent(readBufferImpl, -valueMarker);
              }
          ));
      LOGGER.finer(() -> "deserializeBuiltInComponent: completed Map=" + map);
      return map;
    } else if (positiveMarker == Constants.ARRAY.marker()) {
      Object array = deserializeArray(readBufferImpl);
      LOGGER.finer(() -> "deserializeBuiltInComponent: completed array");
      return array;
    } else {
      throw new IllegalArgumentException("Unsupported built-in marker: " + positiveMarker);
    }
  }

  /// Deserialize an array (primitive or object arrays) - ported from working Machinary.java
  private Object deserializeArray(ReadBufferImpl readBufferImpl) {
    LOGGER.finer(() -> "deserializeArray: starting");

    // Read array type marker to determine what kind of array this is
    byte arrayTypeMarker = readBufferImpl.buffer.get();
    LOGGER.finer(() -> "deserializeArray: arrayTypeMarker=" + arrayTypeMarker);

    Constants arrayType = Constants.fromMarker(arrayTypeMarker);
    LOGGER.finer(() -> "deserializeArray: arrayType=" + arrayType);

    switch (arrayType) {
      case BYTE -> {
        int length = ZigZagEncoding.getInt(readBufferImpl.buffer);
        byte[] bytes = new byte[length];
        readBufferImpl.buffer.get(bytes);
        LOGGER.finer(() -> "deserializeArray: byte[] length=" + bytes.length);
        return bytes;
      }
      case BOOLEAN -> {
        int boolLength = ZigZagEncoding.getInt(readBufferImpl.buffer);
        boolean[] booleans = new boolean[boolLength];
        int bytesLength = ZigZagEncoding.getInt(readBufferImpl.buffer);
        byte[] bytes = new byte[bytesLength];
        readBufferImpl.buffer.get(bytes);
        java.util.BitSet bitSet = java.util.BitSet.valueOf(bytes);
        java.util.stream.IntStream.range(0, boolLength).forEach(i -> {
          booleans[i] = bitSet.get(i);
        });
        LOGGER.finer(() -> "deserializeArray: boolean[] length=" + boolLength);
        return booleans;
      }
      case SHORT -> {
        int length = ZigZagEncoding.getInt(readBufferImpl.buffer);
        short[] shorts = new short[length];
        java.util.stream.IntStream.range(0, length)
            .forEach(i -> shorts[i] = readBufferImpl.buffer.getShort());
        LOGGER.finer(() -> "deserializeArray: short[] length=" + length);
        return shorts;
      }
      case INTEGER -> {
        int length = ZigZagEncoding.getInt(readBufferImpl.buffer);
        int[] integers = new int[length];
        java.util.Arrays.setAll(integers, i -> readBufferImpl.buffer.getInt());
        LOGGER.finer(() -> "deserializeArray: int[] length=" + length);
        return integers;
      }
      case LONG -> {
        int length = ZigZagEncoding.getInt(readBufferImpl.buffer);
        long[] longs = new long[length];
        java.util.Arrays.setAll(longs, i -> readBufferImpl.buffer.getLong());
        LOGGER.finer(() -> "deserializeArray: long[] length=" + length);
        return longs;
      }
      case FLOAT -> {
        int length = ZigZagEncoding.getInt(readBufferImpl.buffer);
        float[] floats = new float[length];
        java.util.stream.IntStream.range(0, length)
            .forEach(i -> floats[i] = readBufferImpl.buffer.getFloat());
        LOGGER.finer(() -> "deserializeArray: float[] length=" + length);
        return floats;
      }
      case DOUBLE -> {
        int length = ZigZagEncoding.getInt(readBufferImpl.buffer);
        double[] doubles = new double[length];
        java.util.Arrays.setAll(doubles, i -> readBufferImpl.buffer.getDouble());
        LOGGER.finer(() -> "deserializeArray: double[] length=" + length);
        return doubles;
      }
      case CHARACTER -> {
        int length = ZigZagEncoding.getInt(readBufferImpl.buffer);
        char[] chars = new char[length];
        java.util.stream.IntStream.range(0, length)
            .forEach(i -> chars[i] = readBufferImpl.buffer.getChar());
        LOGGER.finer(() -> "deserializeArray: char[] length=" + length);
        return chars;
      }
      case STRING -> {
        int length = ZigZagEncoding.getInt(readBufferImpl.buffer);
        String[] strings = new String[length];
        java.util.Arrays.setAll(strings, i -> {
          int strLength = ZigZagEncoding.getInt(readBufferImpl.buffer);
          byte[] bytes = new byte[strLength];
          readBufferImpl.buffer.get(bytes);
          return new String(bytes, java.nio.charset.StandardCharsets.UTF_8);
        });
        LOGGER.finer(() -> "deserializeArray: String[] length=" + length);
        return strings;
      }
      case RECORD -> {
        int length = ZigZagEncoding.getInt(readBufferImpl.buffer);
        LOGGER.finer(() -> "Read RECORD Array len=" + length);

        // Read first element to determine component type from its ordinal
        if (length == 0) {
          // Empty array - return Object[] since we can't determine component type
          return new Object[0];
        }

        // Peek at first element to determine component type
        int firstMarker = ZigZagEncoding.getInt(readBufferImpl.buffer);
        Class<?> componentType;

        if (firstMarker == 0) {
          // First element is null - can't determine type, use Object[]
          componentType = Object.class;
        } else if (firstMarker > 0) {
          // User type - get class from ordinal lookup
          int physicalIndex = firstMarker - 1; // Convert logical ordinal to array index
          componentType = analysis.discoveredClasses()[physicalIndex];
        } else {
          // Built-in type - use Object[] as fallback
          componentType = Object.class;
        }

        // Create properly typed array using component type
        Object array = Array.newInstance(componentType, length);

        // Process first element (already read the marker)
        if (firstMarker == 0) {
          Array.set(array, 0, null);
        } else if (firstMarker > 0) {
          // Put marker back and deserialize
          readBufferImpl.buffer.position(readBufferImpl.buffer.position() - ZigZagEncoding.sizeOf(firstMarker));
          Object element = deserialize(readBufferImpl);
          Array.set(array, 0, element);
        } else {
          Object element = deserializeBuiltInComponent(readBufferImpl, -firstMarker);
          Array.set(array, 0, element);
        }

        // Process remaining elements
        for (int i = 1; i < length; i++) {
          int marker = ZigZagEncoding.getInt(readBufferImpl.buffer);
          Object element;
          if (marker == 0) {
            element = null;
          } else if (marker > 0) {
            readBufferImpl.buffer.position(readBufferImpl.buffer.position() - ZigZagEncoding.sizeOf(marker));
            element = deserialize(readBufferImpl);
          } else {
            element = deserializeBuiltInComponent(readBufferImpl, -marker);
          }
          Array.set(array, i, element);
        }

        LOGGER.finer(() -> "deserializeArray: " + componentType.getSimpleName() + "[] length=" + length + " (ordinal-based)");
        return array;
      }
      case OPTIONAL_OF -> {
        int length = ZigZagEncoding.getInt(readBufferImpl.buffer);
        LOGGER.finer(() -> "Read Optional Array len=" + length);
        java.util.Optional<?>[] optionals = new java.util.Optional<?>[length];
        java.util.stream.IntStream.range(0, length).forEach(i -> {
          byte optMarker = readBufferImpl.buffer.get();
          if (optMarker == Constants.OPTIONAL_EMPTY.marker()) {
            optionals[i] = java.util.Optional.empty();
          } else if (optMarker == Constants.OPTIONAL_OF.marker()) {
            // Read the next value based on its type marker
            byte valueMarker = readBufferImpl.buffer.get();
            readBufferImpl.buffer.position(readBufferImpl.buffer.position() - 1); // Rewind to read marker again
            Object value = switch (Constants.fromMarker(valueMarker)) {
              case STRING -> {
                int strLength = ZigZagEncoding.getInt(readBufferImpl.buffer);
                byte[] bytes = new byte[strLength];
                readBufferImpl.buffer.get(bytes);
                yield new String(bytes, java.nio.charset.StandardCharsets.UTF_8);
              }
              case INTEGER, INTEGER_VAR -> {
                if (valueMarker == Constants.INTEGER.marker()) {
                  yield readBufferImpl.buffer.getInt();
                } else {
                  yield ZigZagEncoding.getInt(readBufferImpl.buffer);
                }
              }
              case LONG, LONG_VAR -> {
                if (valueMarker == Constants.LONG.marker()) {
                  yield readBufferImpl.buffer.getLong();
                } else {
                  yield ZigZagEncoding.getLong(readBufferImpl.buffer);
                }
              }
              case BOOLEAN -> readBufferImpl.buffer.get() != 0;
              case BYTE -> readBufferImpl.buffer.get();
              case SHORT -> readBufferImpl.buffer.getShort();
              case CHARACTER -> readBufferImpl.buffer.getChar();
              case FLOAT -> readBufferImpl.buffer.getFloat();
              case DOUBLE -> readBufferImpl.buffer.getDouble();
              case UUID -> {
                long mostSigBits = readBufferImpl.buffer.getLong();
                long leastSigBits = readBufferImpl.buffer.getLong();
                yield new java.util.UUID(mostSigBits, leastSigBits);
              }
              default ->
                  throw new IllegalArgumentException("Unsupported Optional value type: " + Constants.fromMarker(valueMarker));
            };
            optionals[i] = java.util.Optional.of(value);
          } else {
            throw new IllegalArgumentException("Invalid Optional marker: " + optMarker);
          }
        });
        return optionals;
      }
      default -> throw new IllegalStateException("Unsupported array type marker: " + arrayTypeMarker);
    }
  }

  @Override
  public T deserialize(ReadBuffer buffer) {
    final var readBufferImpl = (ReadBufferImpl) buffer;
    LOGGER.finer(() -> "PicklerImpl.deserialize: starting at position=" + readBufferImpl.buffer.position());

    // Read the logical ordinal/marker
    int logicalOrdinal = ZigZagEncoding.getInt(readBufferImpl.buffer);
    LOGGER.finer(() -> "PicklerImpl.deserialize: read logicalOrdinal=" + logicalOrdinal);

    if (logicalOrdinal == 0) {
      // NULL marker - safe for uninitialized memory
      LOGGER.finer(() -> "PicklerImpl.deserialize: found NULL marker, returning null");
      return null;
    }

    // For positive logical ordinals, convert to physical array index and look up the user type
    if (logicalOrdinal > 0) {
      int physicalIndex = logicalOrdinal - 1; // Convert: logical ordinal 1 -> physical array index 0
      LOGGER.finer(() -> "PicklerImpl.deserialize: logicalOrdinal=" + logicalOrdinal + " physicalIndex=" + physicalIndex);
      if (physicalIndex >= analysis.discoveredClasses().length) {
        throw new IllegalArgumentException("Invalid user type logicalOrdinal: " + logicalOrdinal + " (max=" + analysis.discoveredClasses().length + ")");
      }

      Class<?> userClass = analysis.discoveredClasses()[physicalIndex];
      Tag tag = analysis.tags()[physicalIndex];
      LOGGER.finer(() -> "PicklerImpl.deserialize: deserializing user type " + userClass.getSimpleName() + " with tag=" + tag + " className=" + userClass.getName());

      return deserializeUserType(readBufferImpl, userClass, tag, physicalIndex);
    } else {
      throw new IllegalArgumentException("Built-in types should not appear at top level, logicalOrdinal=" + logicalOrdinal);
    }
  }

  @Override
  public int maxSizeOf(T object) {
    // TODO: Calculate actual size - for now return reasonable default
    return 8192; // 8KB should be plenty for most records
  }

  @Override
  public WriteBuffer allocateForWriting(int bytes) {
    ByteBuffer buffer = ByteBuffer.allocate(bytes);
    buffer.order(java.nio.ByteOrder.BIG_ENDIAN);
    // For now, use a dummy class-to-name function since we won't need class name compression
    return new WriteBufferImpl(buffer, clazz -> clazz.getSimpleName());
  }

  @Override
  public ReadBuffer wrapForReading(ByteBuffer bytes) {
    bytes.order(java.nio.ByteOrder.BIG_ENDIAN);
    // For now, use a dummy ordinal-to-class function since we won't need it yet
    return new ReadBufferImpl(bytes, ordinal -> null);
  }

  @Override
  public ReadBuffer allocateForReading(int size) {
    ByteBuffer buffer = ByteBuffer.allocate(size);
    buffer.order(java.nio.ByteOrder.BIG_ENDIAN);
    return new ReadBufferImpl(buffer, ordinal -> null);
  }

  @Override
  public WriteBuffer wrapForWriting(ByteBuffer buf) {
    buf.order(java.nio.ByteOrder.BIG_ENDIAN);
    return new WriteBufferImpl(buf, clazz -> clazz.getSimpleName());
  }
}
