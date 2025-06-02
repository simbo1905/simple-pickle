// SPDX-FileCopyrightText: 2025 Simon Massey
// SPDX-License-Identifier: MIT

package io.github.simbo1905.no.framework;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Array;
import java.lang.reflect.RecordComponent;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static io.github.simbo1905.no.framework.Pickler.LOGGER;

/// Unified pickler implementation that handles all reachable types using array-based architecture.
/// Eliminates the need for separate RecordPickler and SealedPickler classes.
final class PicklerImpl<T> implements Pickler<T> {

  // Global lookup tables indexed by ordinal - the core of the unified architecture
  final Class<?>[] discoveredClasses;     // Lexicographically sorted user types
  final Tag[] tags;                       // Corresponding type tags (RECORD, ENUM, etc.)
  final MethodHandle[] constructors;      // Sparse array, only populated for records
  final MethodHandle[][] componentAccessors; // Sparse array of accessor arrays for records
  final BiConsumer<ByteBuffer, Object>[] writers;   // Component writers by index
  final Function<ByteBuffer, Object>[] readers;      // Component readers by index
  final Map<Class<?>, Integer> classToOrdinal;       // Fast lookup: class -> array index

  /// Create a unified pickler for any root type (record, enum, or sealed interface)
  PicklerImpl(Class<T> rootClass) {
    Objects.requireNonNull(rootClass, "rootClass cannot be null");

    LOGGER.info(() -> "Creating unified pickler for root class: " + rootClass.getName());

    // Phase 1: Discover all reachable user types using existing recordClassHierarchy
    Set<Class<?>> allReachableClasses = Companion.recordClassHierarchy(rootClass, new HashSet<>())
        .filter(clazz -> clazz.isRecord() || clazz.isEnum() || clazz.isSealed())
        .collect(Collectors.toSet());

    LOGGER.info(() -> "Discovered " + allReachableClasses.size() + " reachable user types: " +
        allReachableClasses.stream().map(Class::getSimpleName).toList());

    // Phase 2: Filter out sealed interfaces (keep only concrete records and enums for serialization)
    this.discoveredClasses = allReachableClasses.stream()
        .filter(clazz -> !clazz.isSealed()) // Remove sealed interfaces - they're only for discovery
        .sorted(Comparator.comparing(Class::getName))
        .toArray(Class<?>[]::new);

    LOGGER.info(() -> "Filtered to " + discoveredClasses.length + " concrete types (removed sealed interfaces)");

    LOGGER.fine(() -> "Lexicographically sorted classes: " +
        Arrays.stream(discoveredClasses).map(Class::getSimpleName).toList());

    // Phase 3: Create class-to-ordinal lookup map (1-indexed logical ordinals)
    this.classToOrdinal = new HashMap<>();
    IntStream.range(0, discoveredClasses.length).forEach(i ->
        classToOrdinal.put(discoveredClasses[i], i + 1) // Logical ordinal = physical index + 1
    );

    // Phase 4: Analyze each type and build corresponding metadata arrays
    this.tags = new Tag[discoveredClasses.length];
    this.constructors = new MethodHandle[discoveredClasses.length];
    this.componentAccessors = new MethodHandle[discoveredClasses.length][];

    IntStream.range(0, discoveredClasses.length).forEach(i -> {
      final var clazz = discoveredClasses[i];
      final var index = i; // effectively final for lambda

      if (clazz.isRecord()) {
        tags[i] = Tag.RECORD;
        constructors[i] = getRecordConstructor(clazz);
        componentAccessors[i] = getRecordComponentAccessors(clazz);
        LOGGER.fine(() -> "Analyzed record " + clazz.getSimpleName() + " at index " + index +
            " with " + componentAccessors[index].length + " components");
      } else if (clazz.isEnum()) {
        tags[i] = Tag.ENUM;
        // constructors[i] and componentAccessors[i] remain null for enums
        LOGGER.fine(() -> "Analyzed enum " + clazz.getSimpleName() + " at index " + index);
      } else {
        throw new IllegalStateException("Unexpected type in filtered array: " + clazz.getName() + " (should be record or enum only)");
      }
    });

    // Phase 5: Build writers and readers arrays (placeholder for now)
    @SuppressWarnings({"unchecked"})
    BiConsumer<ByteBuffer, Object>[] writersArray = new BiConsumer[discoveredClasses.length];
    this.writers = writersArray;
    @SuppressWarnings({"unchecked"})
    Function<ByteBuffer, Object>[] readersArray = new Function[discoveredClasses.length];
    this.readers = readersArray;

    // TODO: Build actual writers and readers using existing TypeStructure.analyze logic
    LOGGER.info(() -> "TODO: Build writers and readers arrays for " + discoveredClasses.length + " types");

    LOGGER.info(() -> "Unified pickler created for " + rootClass.getSimpleName() +
        " with " + discoveredClasses.length + " total reachable types");
  }

  /// Get the logical ordinal (1-indexed) for a given user type class
  int getOrdinal(Class<?> clazz) {
    Integer ordinal = classToOrdinal.get(clazz);
    if (ordinal == null) {
      throw new IllegalArgumentException("Unknown user type: " + clazz.getName());
    }
    return ordinal;
  }

  /// Get the canonical constructor method handle for a record class
  static MethodHandle getRecordConstructor(Class<?> recordClass) {
    try {
      RecordComponent[] components = recordClass.getRecordComponents();
      Class<?>[] parameterTypes = Arrays.stream(components)
          .map(RecordComponent::getType)
          .toArray(Class<?>[]::new);

      var constructor = recordClass.getDeclaredConstructor(parameterTypes);
      return MethodHandles.lookup().unreflectConstructor(constructor);
    } catch (Exception e) {
      throw new RuntimeException("Failed to get constructor for record: " + recordClass.getName(), e);
    }
  }

  /// Get method handles for all record component accessors
  static MethodHandle[] getRecordComponentAccessors(Class<?> recordClass) {
    try {
      RecordComponent[] components = recordClass.getRecordComponents();
      MethodHandle[] accessors = new MethodHandle[components.length];

      IntStream.range(0, components.length).forEach(i -> {
        try {
          accessors[i] = MethodHandles.lookup().unreflect(components[i].getAccessor());
        } catch (IllegalAccessException e) {
          throw new RuntimeException("Failed to unreflect accessor for component " + components[i].getName() + " in record " + recordClass.getName(), e);
        }
      });

      return accessors;
    } catch (Exception e) {
      throw new RuntimeException("Failed to get accessors for record: " + recordClass.getName(), e);
    }
  }

  @Override
  public int serialize(ByteBuffer buffer, T object) {
    final var startPosition = buffer.position();
    LOGGER.finer(() -> "PicklerImpl.serialize: startPosition=" + startPosition + " object=" + object);

    if (object == null) {
      ZigZagEncoding.putInt(buffer, 0); // NULL marker
      LOGGER.finer(() -> "PicklerImpl.serialize: wrote NULL marker, bytes=" + (buffer.position() - startPosition));
      return buffer.position() - startPosition;
    }

    // Get concrete runtime type and its ordinal
    @SuppressWarnings("unchecked")
    Class<? extends T> concreteType = (Class<? extends T>) object.getClass();
    LOGGER.finer(() -> "PicklerImpl.serialize: concreteType=" + concreteType.getSimpleName());

    int logicalOrdinal = getOrdinal(concreteType);  // This returns logical ordinal (1-indexed)
    int physicalIndex = logicalOrdinal - 1;        // Convert to physical index (0-indexed)
    LOGGER.finer(() -> "PicklerImpl.serialize: physicalIndex=" + physicalIndex + " logicalOrdinal=" + logicalOrdinal);

    // Write 1-indexed logical ordinal for user types (1, 2, 3, ...)
    ZigZagEncoding.putInt(buffer, logicalOrdinal);
    LOGGER.finer(() -> "PicklerImpl.serialize: wrote logicalOrdinal=" + logicalOrdinal + " position=" + buffer.position());

    // Serialize based on tag (using physical array index)
    Tag tag = tags[physicalIndex];
    LOGGER.finer(() -> "PicklerImpl.serialize: tag=" + tag + " for physicalIndex=" + physicalIndex + " object=" + object);

    // Handle serialization by tag type
    switch (tag) {
      case ENUM -> {
        // For enums, write the constant name after the logical ordinal
        LOGGER.finer(() -> "PicklerImpl.serialize: serializing enum " + object);
        Enum<?> enumValue = (Enum<?>) object;
        String constantName = enumValue.name();
        byte[] nameBytes = constantName.getBytes(StandardCharsets.UTF_8);
        ZigZagEncoding.putInt(buffer, nameBytes.length);
        buffer.put(nameBytes);
        LOGGER.finer(() -> "PicklerImpl.serialize: wrote enum constant=" + constantName + " length=" + nameBytes.length);
      }
      case RECORD -> {
        // For records, serialize all components
        LOGGER.finer(() -> "PicklerImpl.serialize: serializing record components for " + object);
        serializeRecordComponents(buffer, concreteType, object, logicalOrdinal);
      }
      default -> throw new IllegalStateException("Unsupported tag for user type: " + tag);
    }

    final var totalBytes = buffer.position() - startPosition;
    LOGGER.finer(() -> "PicklerImpl.serialize: completed, totalBytes=" + totalBytes);
    return totalBytes;
  }

  /// Serialize all components of a record using method handles and built-in type writers
  void serializeRecordComponents(ByteBuffer buffer, Class<?> recordClass, Object record, int ordinal) {
    LOGGER.finer(() -> "*** serializeRecordComponents: starting for " + recordClass.getSimpleName() + " ordinal=" + ordinal + " recordClass=" + recordClass.getName() + " actualRecord=" + record.getClass().getName()); // TODO revert to FINER logging after bug fix

    // Get component accessors for this record type from analysis
    int physicalIndex = ordinal - 1; // Convert logical ordinal to physical array index
    var accessors = componentAccessors[physicalIndex];
    LOGGER.finer(() -> "*** serializeRecordComponents: ordinal=" + ordinal + " physicalIndex=" + physicalIndex + " accessors.length=" + (accessors != null ? accessors.length : "null")); // TODO revert to FINER logging after bug fix
    if (accessors == null) {
      LOGGER.finer(() -> "serializeRecordComponents: no components for " + recordClass.getSimpleName());
      return;
    }

    LOGGER.finer(() -> "serializeRecordComponents: found " + accessors.length + " components");

    // Serialize each component using its accessor
    IntStream.range(0, accessors.length).forEach(componentIndex -> {
      try {
        var accessor = accessors[componentIndex];
        Object componentValue = accessor.invoke(record);
        LOGGER.finer(() -> "*** serializeRecordComponents: component[" + componentIndex + "] value=" + componentValue + " type=" + (componentValue != null ? componentValue.getClass().getSimpleName() : "null") + " className=" + (componentValue != null ? componentValue.getClass().getName() : "null")); // TODO revert to FINER logging after bug fix

        // Check if this is a user type or built-in type
        boolean result = false;
        if (componentValue != null) {
          LOGGER.finer(() -> "*** serializeRecordComponents: checking if component[" + componentIndex + "] is user type, class=" + componentValue.getClass().getName()); // TODO revert to FINER logging after bug fix
          try {
            int componentOrdinal = getOrdinal(componentValue.getClass());
            LOGGER.finer(() -> "*** serializeRecordComponents: component[" + componentIndex + "] is user type with ordinal=" + componentOrdinal); // TODO revert to FINER logging after bug fix
            result = true;
          } catch (IllegalArgumentException e) {
            result = false;
          }
        }
        if (componentValue != null && result) {
          // Recursively serialize user types using their ordinals
          LOGGER.finer(() -> "*** serializeRecordComponents: component[" + componentIndex + "] serializing as user type " + componentValue.getClass().getSimpleName()); // TODO revert to FINER logging after bug fix
          serializeUserType(buffer, componentValue);
        } else {
          // Handle built-in types directly
          LOGGER.finer(() -> "*** serializeRecordComponents: component[" + componentIndex + "] serializing as built-in type, componentValue=" + (componentValue != null ? componentValue.getClass().getSimpleName() : "null")); // TODO revert to FINER logging after bug fix
          serializeBuiltInComponent(buffer, componentValue);
        }

      } catch (Throwable e) {
        throw new RuntimeException("Failed to serialize component " + componentIndex + " of " + recordClass.getSimpleName(), e);
      }
    });

    LOGGER.finer(() -> "serializeRecordComponents: completed for " + recordClass.getSimpleName());
  }

  /// Serialize a component value that is a built-in type (String, int, etc.)
  void serializeBuiltInComponent(ByteBuffer buffer, Object componentValue) {
    LOGGER.finer(() -> "serializeBuiltInComponent: value=" + componentValue + " type=" + (componentValue != null ? componentValue.getClass().getSimpleName() : "null"));

    if (componentValue == null) {
      ZigZagEncoding.putInt(buffer, 0); // NULL marker
      LOGGER.finer(() -> "serializeBuiltInComponent: wrote NULL marker");
      return;
    }

    // Handle built-in types with negative markers (using Constants mapping)
    switch (componentValue) {
      case String str -> {
        ZigZagEncoding.putInt(buffer, Constants.STRING.wireMarker());
        byte[] bytes = str.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        ZigZagEncoding.putInt(buffer, bytes.length);
        buffer.put(bytes);
        LOGGER.finer(() -> "serializeBuiltInComponent: wrote String length=" + bytes.length);
      }
      case Integer intValue -> {
        if (ZigZagEncoding.sizeOf(intValue) < Integer.BYTES) {
          ZigZagEncoding.putInt(buffer, Constants.INTEGER_VAR.wireMarker());
          ZigZagEncoding.putInt(buffer, intValue);
        } else {
          ZigZagEncoding.putInt(buffer, Constants.INTEGER.wireMarker());
          buffer.putInt(intValue);
        }
        LOGGER.finer(() -> "serializeBuiltInComponent: wrote Integer=" + intValue);
      }
      case Boolean boolValue -> {
        ZigZagEncoding.putInt(buffer, Constants.BOOLEAN.wireMarker());
        buffer.put((byte) (boolValue ? 1 : 0));
        LOGGER.finer(() -> "serializeBuiltInComponent: wrote Boolean=" + boolValue);
      }
      case Long longValue -> {
        if (ZigZagEncoding.sizeOf(longValue) < Long.BYTES) {
          ZigZagEncoding.putInt(buffer, Constants.LONG_VAR.wireMarker());
          ZigZagEncoding.putLong(buffer, longValue);
        } else {
          ZigZagEncoding.putInt(buffer, Constants.LONG.wireMarker());
          buffer.putLong(longValue);
        }
        LOGGER.finer(() -> "serializeBuiltInComponent: wrote Long=" + longValue);
      }
      case Float floatValue -> {
        ZigZagEncoding.putInt(buffer, Constants.FLOAT.wireMarker());
        buffer.putFloat(floatValue);
        LOGGER.finer(() -> "serializeBuiltInComponent: wrote Float=" + floatValue);
      }
      case Double doubleValue -> {
        ZigZagEncoding.putInt(buffer, Constants.DOUBLE.wireMarker());
        buffer.putDouble(doubleValue);
        LOGGER.finer(() -> "serializeBuiltInComponent: wrote Double=" + doubleValue);
      }
      case Byte byteValue -> {
        ZigZagEncoding.putInt(buffer, Constants.BYTE.wireMarker());
        buffer.put(byteValue);
        LOGGER.finer(() -> "serializeBuiltInComponent: wrote Byte=" + byteValue);
      }
      case Short shortValue -> {
        ZigZagEncoding.putInt(buffer, Constants.SHORT.wireMarker());
        buffer.putShort(shortValue);
        LOGGER.finer(() -> "serializeBuiltInComponent: wrote Short=" + shortValue);
      }
      case Character charValue -> {
        ZigZagEncoding.putInt(buffer, Constants.CHARACTER.wireMarker());
        buffer.putChar(charValue);
        LOGGER.finer(() -> "serializeBuiltInComponent: wrote Character=" + charValue);
      }
      case java.util.UUID uuidValue -> {
        ZigZagEncoding.putInt(buffer, Constants.UUID.wireMarker());
        buffer.putLong(uuidValue.getMostSignificantBits());
        buffer.putLong(uuidValue.getLeastSignificantBits());
        LOGGER.finer(() -> "serializeBuiltInComponent: wrote UUID=" + uuidValue);
      }
      case java.util.Optional<?> optional -> {
        if (optional.isEmpty()) {
          ZigZagEncoding.putInt(buffer, Constants.OPTIONAL_EMPTY.wireMarker());
          LOGGER.finer(() -> "serializeBuiltInComponent: wrote Optional.empty()");
        } else {
          ZigZagEncoding.putInt(buffer, Constants.OPTIONAL_OF.wireMarker());
          // Recursively serialize the value inside the Optional
          serializeBuiltInComponent(buffer, optional.get());
          LOGGER.finer(() -> "serializeBuiltInComponent: wrote Optional.of(" + optional.get() + ")");
        }
      }
      case java.util.List<?> list -> {
        ZigZagEncoding.putInt(buffer, Constants.LIST.wireMarker());
        ZigZagEncoding.putInt(buffer, list.size());
        LOGGER.finer(() -> "serializeBuiltInComponent: wrote List size=" + list.size());
        for (Object item : list) {
          serializeBuiltInComponent(buffer, item);
        }
        LOGGER.finer(() -> "serializeBuiltInComponent: completed List serialization");
      }
      case java.util.Map<?, ?> map -> {
        ZigZagEncoding.putInt(buffer, Constants.MAP.wireMarker());
        ZigZagEncoding.putInt(buffer, map.size());
        LOGGER.finer(() -> "serializeBuiltInComponent: wrote Map size=" + map.size());
        for (var entry : map.entrySet()) {
          serializeBuiltInComponent(buffer, entry.getKey());
          serializeBuiltInComponent(buffer, entry.getValue());
        }
        LOGGER.finer(() -> "serializeBuiltInComponent: completed Map serialization");
      }
      default -> {
        // Check if it's an array
        if (componentValue.getClass().isArray()) {
          ZigZagEncoding.putInt(buffer, Constants.ARRAY.wireMarker());
          LOGGER.finer(() -> "serializeArray: value=" + componentValue + " class=" + componentValue.getClass().getSimpleName());

          switch (componentValue) {
            case byte[] arr -> {
              buffer.put(Constants.BYTE.marker());
              ZigZagEncoding.putInt(buffer, arr.length);
              buffer.put(arr);
              LOGGER.finer(() -> "serializeArray: byte[] length=" + arr.length);
            }
            case boolean[] booleans -> {
              buffer.put(Constants.BOOLEAN.marker());
              int length = booleans.length;
              ZigZagEncoding.putInt(buffer, length);
              BitSet bitSet = new BitSet(length);
              IntStream.range(0, length)
                  .filter(i -> booleans[i])
                  .forEach(bitSet::set);
              byte[] bytes = bitSet.toByteArray();
              ZigZagEncoding.putInt(buffer, bytes.length);
              buffer.put(bytes);
              LOGGER.finer(() -> "serializeArray: boolean[] length=" + length);
            }
            case short[] shorts -> {
              buffer.put(Constants.SHORT.marker());
              ZigZagEncoding.putInt(buffer, shorts.length);
              for (short s : shorts) {
                buffer.putShort(s);
              }
              LOGGER.finer(() -> "serializeArray: short[] length=" + shorts.length);
            }
            case int[] integers -> {
              buffer.put(Constants.INTEGER.marker());
              ZigZagEncoding.putInt(buffer, integers.length);
              for (int i : integers) {
                buffer.putInt(i);
              }
              LOGGER.finer(() -> "serializeArray: int[] length=" + integers.length);
            }
            case long[] longs -> {
              buffer.put(Constants.LONG.marker());
              ZigZagEncoding.putInt(buffer, longs.length);
              for (long l : longs) {
                buffer.putLong(l);
              }
              LOGGER.finer(() -> "serializeArray: long[] length=" + longs.length);
            }
            case float[] floats -> {
              buffer.put(Constants.FLOAT.marker());
              ZigZagEncoding.putInt(buffer, floats.length);
              for (float f : floats) {
                buffer.putFloat(f);
              }
              LOGGER.finer(() -> "serializeArray: float[] length=" + floats.length);
            }
            case double[] doubles -> {
              buffer.put(Constants.DOUBLE.marker());
              ZigZagEncoding.putInt(buffer, doubles.length);
              for (double d : doubles) {
                buffer.putDouble(d);
              }
              LOGGER.finer(() -> "serializeArray: double[] length=" + doubles.length);
            }
            case char[] chars -> {
              buffer.put(Constants.CHARACTER.marker());
              ZigZagEncoding.putInt(buffer, chars.length);
              for (char c : chars) {
                buffer.putChar(c);
              }
              LOGGER.finer(() -> "serializeArray: char[] length=" + chars.length);
            }
            case String[] strings -> {
              buffer.put(Constants.STRING.marker());
              ZigZagEncoding.putInt(buffer, strings.length);
              for (String s : strings) {
                byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
                ZigZagEncoding.putInt(buffer, bytes.length);
                buffer.put(bytes);
              }
              LOGGER.finer(() -> "serializeArray: String[] length=" + strings.length);
            }
            default -> {
              // Generic object array - handle Record[] and Optional[] arrays
              Class<?> componentType = componentValue.getClass().getComponentType();

              if (componentType == Optional.class) {
                // Handle Optional[] arrays
                Optional<?>[] optionals = (Optional<?>[]) componentValue;
                buffer.put(Constants.OPTIONAL_OF.marker());
                ZigZagEncoding.putInt(buffer, optionals.length);
                for (Optional<?> opt : optionals) {
                  if (opt.isEmpty()) {
                    buffer.put(Constants.OPTIONAL_EMPTY.marker());
                    LOGGER.finer(() -> "serializeArray: wrote OPTIONAL_EMPTY marker=" + Constants.OPTIONAL_EMPTY.marker());
                  } else {
                    buffer.put(Constants.OPTIONAL_OF.marker());
                    LOGGER.finer(() -> "serializeArray: wrote OPTIONAL_OF marker=" + Constants.OPTIONAL_OF.marker() + " for value=" + opt.get());
                    serializeBuiltInComponent(buffer, opt.get());
                  }
                }
                LOGGER.finer(() -> "serializeArray: Optional[] length=" + optionals.length);
              } else {
                // Handle Record[] arrays and other object arrays using ordinals (no class names)
                Object[] objectArray = (Object[]) componentValue;
                buffer.put(Constants.RECORD.marker()); // Use RECORD marker for object arrays
                
                // Write component type ordinal for empty array support
                Class<?> arrayComponentType = componentValue.getClass().getComponentType();
                
                // Check if component type is user type or built-in type
                boolean isUserType;
                try {
                  getOrdinal(arrayComponentType);
                  isUserType = true;
                } catch (IllegalArgumentException e) {
                  isUserType = false;
                }
                
                if (isUserType) {
                  int componentOrdinal = getOrdinal(arrayComponentType);
                  LOGGER.info(() -> "*** Writing user type ordinal=" + componentOrdinal + " for componentType=" + arrayComponentType.getSimpleName()); // TODO revert to FINER logging after bug fix
                  ZigZagEncoding.putInt(buffer, componentOrdinal); // Positive ordinal for user types
                } else {
                  // Built-in type - use negative marker
                  Constants constant = Constants.fromClass(arrayComponentType);
                  LOGGER.info(() -> "*** Writing built-in wireMarker=" + constant.wireMarker() + " for componentType=" + arrayComponentType.getSimpleName()); // TODO revert to FINER logging after bug fix
                  ZigZagEncoding.putInt(buffer, constant.wireMarker()); // Negative marker for built-in types
                }
                
                ZigZagEncoding.putInt(buffer, objectArray.length);

                // NO class name serialization in unified pickler - use ordinals only
                for (Object item : objectArray) {
                  if (item == null) {
                    ZigZagEncoding.putInt(buffer, 0); // NULL marker
                  } else {
                    boolean result;
                    try {
                      getOrdinal(item.getClass());
                      result = true;
                    } catch (IllegalArgumentException e) {
                      result = false;
                    }
                    if (result) {
                      serializeUserType(buffer, item);
                    } else {
                      serializeBuiltInComponent(buffer, item);
                    }
                  }
                }
                LOGGER.finer(() -> "serializeArray: " + componentType.getSimpleName() + "[] length=" + objectArray.length + " (ordinal-based)");
              }
            }
          }
        } else {
          throw new IllegalArgumentException("Unsupported built-in component type: " + componentValue.getClass());
        }
      }
    }
  }

  /// Serialize a user type (record/enum) using its ordinal from analysis
  void serializeUserType(ByteBuffer buffer, Object userTypeValue) {
    LOGGER.finer(() -> "serializeUserType: value=" + userTypeValue + " type=" + userTypeValue.getClass().getSimpleName() + " className=" + userTypeValue.getClass().getName());

    Class<?> userClass = userTypeValue.getClass();
    int physicalIndex = getOrdinal(userClass);
    int logicalOrdinal = physicalIndex + 1; // Convert: physical array index 0 -> logical ordinal 1
    Tag tag = tags[physicalIndex];

    LOGGER.finer(() -> "serializeUserType: physicalIndex=" + physicalIndex + " logicalOrdinal=" + logicalOrdinal + " tag=" + tag + " userClass=" + userClass.getName());

    // Write the logical ordinal for this user type
    ZigZagEncoding.putInt(buffer, logicalOrdinal);

    // Handle based on tag type
    switch (tag) {
      case ENUM -> {
        // For enums, serialize the constant name
        Enum<?> enumValue = (Enum<?>) userTypeValue;
        String constantName = enumValue.name();
        byte[] nameBytes = constantName.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        ZigZagEncoding.putInt(buffer, nameBytes.length);
        buffer.put(nameBytes);
        LOGGER.finer(() -> "serializeUserType: wrote enum constant=" + constantName);
      }
      case RECORD -> {
        // For records, recursively serialize components
        LOGGER.finer(() -> "serializeUserType: recursively serializing record components");
        serializeRecordComponents(buffer, userClass, userTypeValue, physicalIndex);
      }
      default -> throw new IllegalStateException("Unsupported user type tag: " + tag);
    }
  }

  /// Deserialize a user type (record/enum) using its ordinal and tag information
  @SuppressWarnings("unchecked")
  T deserializeUserType(ByteBuffer buffer, Class<?> userClass, Tag tag, int ordinal) {
    LOGGER.finer(() -> "deserializeUserType: class=" + userClass.getSimpleName() + " className=" + userClass.getName() + " tag=" + tag + " ordinal=" + ordinal);

    return switch (tag) {
      case ENUM -> {
        // For enums, read the constant name and look it up
        int nameLength = ZigZagEncoding.getInt(buffer);
        byte[] nameBytes = new byte[nameLength];
        buffer.get(nameBytes);
        String constantName = new String(nameBytes, java.nio.charset.StandardCharsets.UTF_8);
        LOGGER.finer(() -> "deserializeUserType: enum constant=" + constantName);

        @SuppressWarnings("rawtypes")
        Enum enumValue = Enum.valueOf((Class<Enum>) userClass, constantName);
        yield (T) enumValue;
      }
      case RECORD -> {
        // For records, deserialize components and invoke constructor
        LOGGER.finer(() -> "deserializeUserType: deserializing record components");
        Object[] componentValues = deserializeRecordComponents(buffer, ordinal);

        try {
          int physicalIndex = ordinal - 1; // Convert logical ordinal to physical array index  
          MethodHandle constructor = constructors[physicalIndex];
          LOGGER.finer(() -> "*** deserializeUserType: ordinal=" + ordinal + " physicalIndex=" + physicalIndex + " constructor"); // TODO revert to FINER logging after bug fix
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
  Object[] deserializeRecordComponents(ByteBuffer buffer, int ordinal) {
    LOGGER.finer(() -> "deserializeRecordComponents: starting for ordinal=" + ordinal);

    // Get component accessors to determine how many components to read
    int physicalIndex = ordinal - 1; // Convert logical ordinal to physical array index
    var accessors = componentAccessors[physicalIndex];
    if (accessors == null) {
      LOGGER.finer(() -> "deserializeRecordComponents: no components");
      return new Object[0];
    }

    LOGGER.finer(() -> "deserializeRecordComponents: reading " + accessors.length + " components");

    // Deserialize each component using functional approach
    Object[] componentValues = IntStream.range(0, accessors.length)
        .mapToObj(componentIndex -> {
          // Read the marker/ordinal for this component
          int marker = ZigZagEncoding.getInt(buffer);
          LOGGER.finer(() -> "deserializeRecordComponents: component[" + componentIndex + "] marker=" + marker);

          if (marker == 0) {
            LOGGER.finer(() -> "deserializeRecordComponents: component[" + componentIndex + "] = null");
            return null;
          } else if (marker < 0) {
            // Built-in type with negative marker
            Object value = deserializeBuiltInComponent(buffer, -marker);
            LOGGER.finer(() -> "deserializeRecordComponents: component[" + componentIndex + "] = " + value + " (built-in)");
            return value;
          } else {
            // User type with positive logical ordinal, convert to physical array index
            int componentPhysicalIndex = marker - 1; // Convert: logical ordinal 1 -> physical array index 0
            Class<?> componentClass = discoveredClasses[componentPhysicalIndex];
            Tag componentTag = tags[componentPhysicalIndex];
            Object value = deserializeUserType(buffer, componentClass, componentTag, marker);
            LOGGER.finer(() -> "deserializeRecordComponents: component[" + componentIndex + "] = " + value + " (user type)");
            return value;
          }
        })
        .toArray();

    LOGGER.finer(() -> "deserializeRecordComponents: completed with " + componentValues.length + " values");
    return componentValues;
  }

  /// Deserialize a component value that is a built-in type using negative marker
  Object deserializeBuiltInComponent(ByteBuffer buffer, int positiveMarker) {
    LOGGER.finer(() -> "deserializeBuiltInComponent: positiveMarker=" + positiveMarker);

    if (positiveMarker == Constants.BOOLEAN.marker()) {
      boolean value = buffer.get() != 0;
      LOGGER.finer(() -> "deserializeBuiltInComponent: read Boolean=" + value);
      return value;
    } else if (positiveMarker == Constants.INTEGER.marker()) {
      int value = ZigZagEncoding.getInt(buffer);
      LOGGER.finer(() -> "deserializeBuiltInComponent: read Integer=" + value);
      return value;
    } else if (positiveMarker == Constants.LONG.marker()) {
      long value = ZigZagEncoding.getLong(buffer);
      LOGGER.finer(() -> "deserializeBuiltInComponent: read Long=" + value);
      return value;
    } else if (positiveMarker == Constants.FLOAT.marker()) {
      float value = buffer.getFloat();
      LOGGER.finer(() -> "deserializeBuiltInComponent: read Float=" + value);
      return value;
    } else if (positiveMarker == Constants.DOUBLE.marker()) {
      double value = buffer.getDouble();
      LOGGER.finer(() -> "deserializeBuiltInComponent: read Double=" + value);
      return value;
    } else if (positiveMarker == Constants.BYTE.marker()) {
      byte value = buffer.get();
      LOGGER.finer(() -> "deserializeBuiltInComponent: read Byte=" + value);
      return value;
    } else if (positiveMarker == Constants.SHORT.marker()) {
      short value = buffer.getShort();
      LOGGER.finer(() -> "deserializeBuiltInComponent: read Short=" + value);
      return value;
    } else if (positiveMarker == Constants.CHARACTER.marker()) {
      char value = buffer.getChar();
      LOGGER.finer(() -> "deserializeBuiltInComponent: read Character=" + value);
      return value;
    } else if (positiveMarker == Constants.UUID.marker()) {
      long mostSigBits = buffer.getLong();
      long leastSigBits = buffer.getLong();
      java.util.UUID value = new java.util.UUID(mostSigBits, leastSigBits);
      LOGGER.finer(() -> "deserializeBuiltInComponent: read UUID=" + value);
      return value;
    } else if (positiveMarker == Constants.STRING.marker()) {
      int length = ZigZagEncoding.getInt(buffer);
      byte[] bytes = new byte[length];
      buffer.get(bytes);
      String value = new String(bytes, java.nio.charset.StandardCharsets.UTF_8);
      LOGGER.finer(() -> "deserializeBuiltInComponent: read String length=" + length + " value=" + value);
      return value;
    } else if (positiveMarker == Constants.OPTIONAL_OF.marker()) {
      Object innerValue = deserializeBuiltInComponent(buffer, -ZigZagEncoding.getInt(buffer));
      java.util.Optional<Object> value = java.util.Optional.of(innerValue);
      LOGGER.finer(() -> "deserializeBuiltInComponent: read Optional.of(" + innerValue + ")");
      return value;
    } else if (positiveMarker == Constants.OPTIONAL_EMPTY.marker()) {
      LOGGER.finer(() -> "deserializeBuiltInComponent: read Optional.empty()");
      return java.util.Optional.empty();
    } else if (positiveMarker == Constants.LIST.marker()) {
      int size = ZigZagEncoding.getInt(buffer);
      LOGGER.finer(() -> "deserializeBuiltInComponent: reading List size=" + size);
      java.util.List<Object> list = IntStream.range(0, size)
          .mapToObj(i -> {
            int itemMarker = ZigZagEncoding.getInt(buffer);
            return itemMarker == 0 ? null : deserializeBuiltInComponent(buffer, -itemMarker);
          })
          .collect(java.util.stream.Collectors.toList());
      LOGGER.finer(() -> "deserializeBuiltInComponent: completed List=" + list);
      return list;
    } else if (positiveMarker == Constants.MAP.marker()) {
      int size = ZigZagEncoding.getInt(buffer);
      LOGGER.finer(() -> "deserializeBuiltInComponent: reading Map size=" + size);
      java.util.Map<Object, Object> map = IntStream.range(0, size)
          .boxed()
          .collect(java.util.stream.Collectors.toMap(
              i -> {
                int keyMarker = ZigZagEncoding.getInt(buffer);
                return keyMarker == 0 ? null : deserializeBuiltInComponent(buffer, -keyMarker);
              },
              i -> {
                int valueMarker = ZigZagEncoding.getInt(buffer);
                return valueMarker == 0 ? null : deserializeBuiltInComponent(buffer, -valueMarker);
              }
          ));
      LOGGER.finer(() -> "deserializeBuiltInComponent: completed Map=" + map);
      return map;
    } else if (positiveMarker == Constants.ARRAY.marker()) {
      Object array = deserializeArray(buffer);
      LOGGER.finer(() -> "deserializeBuiltInComponent: completed array");
      return array;
    } else {
      throw new IllegalArgumentException("Unsupported built-in marker: " + positiveMarker);
    }
  }

  /// Deserialize an array (primitive or object arrays) - ported from working Machinary.java
  Object deserializeArray(ByteBuffer buffer) {
    LOGGER.finer(() -> "deserializeArray: starting");

    // Read array type marker to determine what kind of array this is
    byte arrayTypeMarker = buffer.get();
    LOGGER.finer(() -> "deserializeArray: arrayTypeMarker=" + arrayTypeMarker);

    Constants arrayType = Constants.fromMarker(arrayTypeMarker);
    LOGGER.finer(() -> "deserializeArray: arrayType=" + arrayType);

    switch (arrayType) {
      case BYTE -> {
        int length = ZigZagEncoding.getInt(buffer);
        byte[] bytes = new byte[length];
        buffer.get(bytes);
        LOGGER.finer(() -> "deserializeArray: byte[] length=" + bytes.length);
        return bytes;
      }
      case BOOLEAN -> {
        int boolLength = ZigZagEncoding.getInt(buffer);
        boolean[] booleans = new boolean[boolLength];
        int bytesLength = ZigZagEncoding.getInt(buffer);
        byte[] bytes = new byte[bytesLength];
        buffer.get(bytes);
        java.util.BitSet bitSet = java.util.BitSet.valueOf(bytes);
        java.util.stream.IntStream.range(0, boolLength).forEach(i -> {
          booleans[i] = bitSet.get(i);
        });
        LOGGER.finer(() -> "deserializeArray: boolean[] length=" + boolLength);
        return booleans;
      }
      case SHORT -> {
        int length = ZigZagEncoding.getInt(buffer);
        short[] shorts = new short[length];
        java.util.stream.IntStream.range(0, length)
            .forEach(i -> shorts[i] = buffer.getShort());
        LOGGER.finer(() -> "deserializeArray: short[] length=" + length);
        return shorts;
      }
      case INTEGER -> {
        int length = ZigZagEncoding.getInt(buffer);
        int[] integers = new int[length];
        java.util.Arrays.setAll(integers, i -> buffer.getInt());
        LOGGER.finer(() -> "deserializeArray: int[] length=" + length);
        return integers;
      }
      case LONG -> {
        int length = ZigZagEncoding.getInt(buffer);
        long[] longs = new long[length];
        java.util.Arrays.setAll(longs, i -> buffer.getLong());
        LOGGER.finer(() -> "deserializeArray: long[] length=" + length);
        return longs;
      }
      case FLOAT -> {
        int length = ZigZagEncoding.getInt(buffer);
        float[] floats = new float[length];
        java.util.stream.IntStream.range(0, length)
            .forEach(i -> floats[i] = buffer.getFloat());
        LOGGER.finer(() -> "deserializeArray: float[] length=" + length);
        return floats;
      }
      case DOUBLE -> {
        int length = ZigZagEncoding.getInt(buffer);
        double[] doubles = new double[length];
        java.util.Arrays.setAll(doubles, i -> buffer.getDouble());
        LOGGER.finer(() -> "deserializeArray: double[] length=" + length);
        return doubles;
      }
      case CHARACTER -> {
        int length = ZigZagEncoding.getInt(buffer);
        char[] chars = new char[length];
        java.util.stream.IntStream.range(0, length)
            .forEach(i -> chars[i] = buffer.getChar());
        LOGGER.finer(() -> "deserializeArray: char[] length=" + length);
        return chars;
      }
      case STRING -> {
        int length = ZigZagEncoding.getInt(buffer);
        String[] strings = new String[length];
        java.util.Arrays.setAll(strings, i -> {
          int strLength = ZigZagEncoding.getInt(buffer);
          byte[] bytes = new byte[strLength];
          buffer.get(bytes);
          return new String(bytes, java.nio.charset.StandardCharsets.UTF_8);
        });
        LOGGER.finer(() -> "deserializeArray: String[] length=" + length);
        return strings;
      }
      case RECORD -> {
        // Read component type marker/ordinal first
        int componentMarker = ZigZagEncoding.getInt(buffer);
        Class<?> componentType;
        
        if (componentMarker > 0) {
          // Positive ordinal - user type
          int physicalIndex = componentMarker - 1; // Convert logical ordinal to array index
          componentType = discoveredClasses[physicalIndex];
          LOGGER.info(() -> "*** Read user type ordinal=" + componentMarker + " physicalIndex=" + physicalIndex + " componentType=" + componentType.getSimpleName()); // TODO revert to FINER logging after bug fix
        } else {
          // Negative marker - built-in type
          Constants constant = Constants.fromMarker((byte) (-componentMarker));
          componentType = constant.clazz;
          LOGGER.info(() -> "*** Read built-in wireMarker=" + componentMarker + " componentType=" + componentType.getSimpleName()); // TODO revert to FINER logging after bug fix
        }
        
        int length = ZigZagEncoding.getInt(buffer);
        LOGGER.finer(() -> "Read RECORD Array len=" + length + " componentType=" + componentType.getSimpleName());

        // Create properly typed array using component type from ordinal
        LOGGER.info(() -> "*** Creating array of componentType=" + componentType.getSimpleName() + " length=" + length); // TODO revert to FINER logging after bug fix
        Object array = Array.newInstance(componentType, length);
        LOGGER.info(() -> "*** Created array: " + array.getClass().getSimpleName()); // TODO revert to FINER logging after bug fix
        
        if (length == 0) {
          LOGGER.info(() -> "*** Returning empty typed array: " + array.getClass().getSimpleName()); // TODO revert to FINER logging after bug fix
          return array; // Return empty typed array
        }

        // Read first element
        int firstMarker = ZigZagEncoding.getInt(buffer);

        // Process first element (already read the marker)
        if (firstMarker == 0) {
          Array.set(array, 0, null);
        } else if (firstMarker > 0) {
          // Put marker back and deserialize
          buffer.position(buffer.position() - ZigZagEncoding.sizeOf(firstMarker));
          Object element = deserialize(buffer);
          Array.set(array, 0, element);
        } else {
          Object element = deserializeBuiltInComponent(buffer, -firstMarker);
          Array.set(array, 0, element);
        }

        // Process remaining elements
        IntStream.range(1, length).forEach(i -> {
          int marker = ZigZagEncoding.getInt(buffer);
          Object element;
          if (marker == 0) {
            element = null;
          } else if (marker > 0) {
            buffer.position(buffer.position() - ZigZagEncoding.sizeOf(marker));
            element = deserialize(buffer);
          } else {
            element = deserializeBuiltInComponent(buffer, -marker);
          }
          Array.set(array, i, element);
        });

        LOGGER.finer(() -> "deserializeArray: " + componentType.getSimpleName() + "[] length=" + length + " (ordinal-based)");
        return array;
      }
      case OPTIONAL_OF -> {
        int length = ZigZagEncoding.getInt(buffer);
        LOGGER.finer(() -> "Read Optional Array len=" + length);
        java.util.Optional<?>[] optionals = new java.util.Optional<?>[length];
        java.util.stream.IntStream.range(0, length).forEach(i -> {
          byte optMarker = buffer.get();
          LOGGER.finer(() -> "deserializeArray: reading Optional[" + i + "] optMarker=" + optMarker);
          if (optMarker == Constants.OPTIONAL_EMPTY.marker()) {
            optionals[i] = java.util.Optional.empty();
            LOGGER.finer(() -> "deserializeArray: created empty Optional[" + i + "]");
          } else if (optMarker == Constants.OPTIONAL_OF.marker()) {
            // Read the next value - negative marker for built-in types
            int negativeMarker = ZigZagEncoding.getInt(buffer);
            byte valueMarker = (byte) (-negativeMarker); // Convert negative marker to positive
            LOGGER.finer(() -> "deserializeArray: negativeMarker=" + negativeMarker + " valueMarker=" + valueMarker);
            Object value = switch (Constants.fromMarker(valueMarker)) {
              case STRING -> {
                int strLength = ZigZagEncoding.getInt(buffer);
                byte[] bytes = new byte[strLength];
                buffer.get(bytes);
                yield new String(bytes, java.nio.charset.StandardCharsets.UTF_8);
              }
              case INTEGER, INTEGER_VAR -> {
                if (valueMarker == Constants.INTEGER.marker()) {
                  yield buffer.getInt();
                } else {
                  yield ZigZagEncoding.getInt(buffer);
                }
              }
              case LONG, LONG_VAR -> {
                if (valueMarker == Constants.LONG.marker()) {
                  yield buffer.getLong();
                } else {
                  yield ZigZagEncoding.getLong(buffer);
                }
              }
              case BOOLEAN -> buffer.get() != 0;
              case BYTE -> buffer.get();
              case SHORT -> buffer.getShort();
              case CHARACTER -> buffer.getChar();
              case FLOAT -> buffer.getFloat();
              case DOUBLE -> buffer.getDouble();
              case UUID -> {
                long mostSigBits = buffer.getLong();
                long leastSigBits = buffer.getLong();
                yield new java.util.UUID(mostSigBits, leastSigBits);
              }
              default ->
                  throw new IllegalArgumentException("Unsupported Optional value type: " + Constants.fromMarker(valueMarker));
            };
            LOGGER.finer(() -> "deserializeArray: created Optional[" + i + "] with value=" + value);
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
  public T deserialize(ByteBuffer buffer) {
    LOGGER.finer(() -> "PicklerImpl.deserialize: starting at position=" + buffer.position());

    // Read the logical ordinal/marker
    int logicalOrdinal = ZigZagEncoding.getInt(buffer);
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
      if (physicalIndex >= discoveredClasses.length) {
        throw new IllegalArgumentException("Invalid user type logicalOrdinal: " + logicalOrdinal + " (max=" + discoveredClasses.length + ")");
      }

      Class<?> userClass = discoveredClasses[physicalIndex];
      Tag tag = tags[physicalIndex];
      LOGGER.finer(() -> "PicklerImpl.deserialize: deserializing user type " + userClass.getSimpleName() + " with tag=" + tag + " className=" + userClass.getName());

      return deserializeUserType(buffer, userClass, tag, logicalOrdinal);
    } else {
      throw new IllegalArgumentException("Built-in types should not appear at top level, logicalOrdinal=" + logicalOrdinal);
    }
  }

  @Override
  public int maxSizeOf(T object) {
    // TODO: Calculate actual size - for now return reasonable default
    return 8192; // 8KB should be plenty for most records
  }

  ByteBuffer allocateForWriting(int bytes) {
    ByteBuffer buffer = ByteBuffer.allocate(bytes);
    buffer.order(java.nio.ByteOrder.BIG_ENDIAN);
    return buffer;
  }
}
