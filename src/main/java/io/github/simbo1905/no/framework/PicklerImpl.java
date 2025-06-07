// SPDX-FileCopyrightText: 2025 Simon Massey
// SPDX-License-Identifier: MIT

package io.github.simbo1905.no.framework;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.*;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.ToIntFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static io.github.simbo1905.no.framework.Constants.INTEGER_VAR;
import static io.github.simbo1905.no.framework.Constants.LONG_VAR;
import static io.github.simbo1905.no.framework.Tag.*;
import static java.nio.charset.StandardCharsets.UTF_8;

/// Unified pickler implementation that handles all reachable types using array-based architecture.
/// Eliminates the need for separate RecordPickler and SealedPickler classes.
final class PicklerImpl<T> implements Pickler<T> {

  static final int SAMPLE_SIZE = 32;

  // Global lookup tables indexed by ordinal - the core of the unified architecture
  final Class<?>[] userTypes;     // Lexicographically sorted user types
  final Map<Class<?>, Integer> classToOrdinal;  // Fast class to ordinal lookup (the ONE HashMap)

  // Pre-built component metadata arrays for all discovered record types - the core performance optimization
  final MethodHandle[] recordConstructors;      // Constructor method handles indexed by ordinal
  final MethodHandle[][] componentAccessors;    // [recordOrdinal][componentIndex] -> accessor method handle
  final TypeStructure[][] componentTypes;       // [recordOrdinal][componentIndex] -> type structure
  final BiConsumer<ByteBuffer, Object>[][] componentWriters;  // [recordOrdinal][componentIndex] -> writer lambda
  final Function<ByteBuffer, Object>[][] componentReaders;    // [recordOrdinal][componentIndex] -> reader lambda
  final ToIntFunction<Object>[][] componentSizers; // [recordOrdinal][componentIndex] -> sizer lambda

  /// Create a unified pickler for any root type (record, enum, or sealed interface)
  @SuppressWarnings("unchecked")
  PicklerImpl(Class<T> rootClass) {
    Objects.requireNonNull(rootClass, "rootClass cannot be null");

    LOGGER.info(() -> "Creating unified pickler for root class: " + rootClass.getName());

    // Phase 1: Static analysis to discover all reachable user types using recordClassHierarchy
    Set<Class<?>> allReachableClasses = recordClassHierarchy(rootClass, new HashSet<>())
        .filter(clazz -> clazz.isRecord() || clazz.isEnum() || clazz.isSealed())
        .collect(Collectors.toSet());

    LOGGER.info(() -> "Discovered " + allReachableClasses.size() + " reachable user types: " +
        allReachableClasses.stream().map(Class::getSimpleName).toList());

    // Phase 2: Filter out sealed interfaces (keep only concrete records and enums for serialization)
    this.userTypes = allReachableClasses.stream()
        .filter(clazz -> !clazz.isSealed()) // Remove sealed interfaces - they're only for discovery
        .sorted(Comparator.comparing(Class::getName))
        .toArray(Class<?>[]::new);

    LOGGER.info(() -> "Filtered to " + userTypes.length + " concrete types (removed sealed interfaces)");

    LOGGER.fine(() -> "Lexicographically sorted classes: " +
        Arrays.stream(userTypes).map(Class::getSimpleName).toList());

    // Build the ONE HashMap for class->ordinal lookup (O(1) for hot path)
    this.classToOrdinal = IntStream.range(0, userTypes.length)
        .boxed()
        .collect(Collectors.toMap(i -> userTypes[i], i -> i));

    LOGGER.finer(() -> "Built classToOrdinal map: " + classToOrdinal.entrySet().stream()
        .map(e -> e.getKey().getSimpleName() + "->" + e.getValue()).toList());

    // Pre-allocate metadata arrays for all discovered record types (array-based for O(1) access)
    int numRecordTypes = userTypes.length;
    this.recordConstructors = new MethodHandle[numRecordTypes];
    this.componentAccessors = new MethodHandle[numRecordTypes][];
    this.componentTypes = new TypeStructure[numRecordTypes][];
    this.componentWriters = new BiConsumer[numRecordTypes][];
    this.componentReaders = new Function[numRecordTypes][];
    this.componentSizers = new ToIntFunction[numRecordTypes][];

    IntStream.range(0, numRecordTypes).forEach(ordinal -> {
      Class<?> recordClass = userTypes[ordinal];
      if (recordClass.isRecord()) {
        try {
          metaprogramming(ordinal, recordClass);
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    });

    LOGGER.info(() -> "PicklerImpl construction complete - ready for high-performance serialization");
  }

  /// Build component metadata for a record type using meta-programming
  @SuppressWarnings("unchecked")
  void metaprogramming(int ordinal, Class<?> recordClass) throws Exception {
    LOGGER.fine(() -> "Building metadata for ordinal " + ordinal + ": " + recordClass.getSimpleName());

    // Get component accessors and analyze types
    RecordComponent[] components = recordClass.getRecordComponents();
    int numComponents = components.length;

    Class<?>[] parameterTypes = Arrays.stream(components)
        .map(RecordComponent::getType)
        .toArray(Class<?>[]::new);
    var constructor = recordClass.getDeclaredConstructor(parameterTypes);
    recordConstructors[ordinal] = MethodHandles.lookup().unreflectConstructor(constructor);
    componentAccessors[ordinal] = new MethodHandle[numComponents];
    componentTypes[ordinal] = new TypeStructure[numComponents];
    componentWriters[ordinal] = new BiConsumer[numComponents];
    componentReaders[ordinal] = new Function[numComponents];
    componentSizers[ordinal] = new ToIntFunction[numComponents];

    IntStream.range(0, numComponents).forEach(i -> {
      RecordComponent component = components[i];

      // Get accessor method handle
      try {
        componentAccessors[ordinal][i] = MethodHandles.lookup().unreflect(component.getAccessor());
      } catch (IllegalAccessException e) {
        throw new RuntimeException("Failed to un reflect accessor for " + component.getName(), e);
      }

      final var typeStructure = TypeStructure.analyze(component.getGenericType());
      assert !typeStructure.tagTypes().isEmpty() : "Type structure for " + component.getName() + " should not be empty";

      // Static analysis of component type structure of the component to understand if it has nested containers
      // like array, list, map, optional,  etc
      componentTypes[ordinal][i] = typeStructure;

      // Build writer, reader, and sizer chains (simplified for now)
      componentWriters[ordinal][i] = buildWriterChain(typeStructure, componentAccessors[ordinal][i]);
      componentReaders[ordinal][i] = buildReaderChain(typeStructure);
      componentSizers[ordinal][i] = buildSizerChain(typeStructure, componentAccessors[ordinal][i]);
    });

    LOGGER.fine(() -> "Completed metadata for " + recordClass.getSimpleName() + " with " + numComponents + " components");
  }

  /// Serialize record components using pre-built writers array (NO HashMap lookups)
  void serializeRecordComponents(ByteBuffer buffer, Record record, int ordinal) {
    BiConsumer<ByteBuffer, Object>[] writers = componentWriters[ordinal];
    if (writers == null) {
      throw new IllegalStateException("No writers for ordinal: " + ordinal);
    }

    LOGGER.finer(() -> "Serializing " + writers.length + " components for " + record.getClass().getSimpleName() +
        " at position " + buffer.position()); // TODO revert to FINER logging after bug fix

    // Use pre-built writers - direct array access, no HashMap lookups
    for (int i = 0; i < writers.length; i++) {
      final int componentIndex = i; // final for lambda capture
      LOGGER.finer(() -> "Writing component " + componentIndex + " at position " + buffer.position()); // TODO revert to FINER logging after bug fix
      writers[i].accept(buffer, record);
      LOGGER.finer(() -> "Finished component " + componentIndex + " at position " + buffer.position()); // TODO revert to FINER logging after bug fix
    }
  }

  /// Deserialize record using pre-built readers and constructor
  @SuppressWarnings("unchecked")
  T deserializeRecord(ByteBuffer buffer, int ordinal) {
    Function<ByteBuffer, Object>[] readers = componentReaders[ordinal];
    MethodHandle constructor = recordConstructors[ordinal];

    if (readers == null || constructor == null) {
      throw new IllegalStateException("No readers/constructor for ordinal: " + ordinal);
    }

    LOGGER.finer(() -> "Deserializing " + readers.length + " components at position " + buffer.position()); // TODO revert to FINER logging after bug fix

    // Read components using pre-built readers
    Object[] components = new Object[readers.length];
    for (int i = 0; i < readers.length; i++) {
      final int componentIndex = i; // final for lambda capture
      LOGGER.finer(() -> "Reading component " + componentIndex + " at position " + buffer.position()); // TODO revert to FINER logging after bug fix
      components[i] = readers[i].apply(buffer);
      final Object componentValue = components[i]; // final for lambda capture
      LOGGER.finer(() -> "Read component " + componentIndex + ": " + componentValue + " at position " + buffer.position()); // TODO revert to FINER logging after bug fix
    }

    // Invoke constructor
    try {
      LOGGER.finer(() -> "Constructing record with components: " + Arrays.toString(components)); // TODO revert to FINER logging after bug fix
      return (T) constructor.invokeWithArguments(components);
    } catch (Throwable e) {
      throw new RuntimeException("Failed to construct record", e);
    }
  }

  /// Calculate max size using pre-built component sizers
  int maxSizeOfRecordComponents(Record record, int ordinal) {
    ToIntFunction<Object>[] sizers = componentSizers[ordinal];
    if (sizers == null) {
      throw new IllegalStateException("No sizers for ordinal: " + ordinal);
    }

    int totalSize = 0;
    for (ToIntFunction<Object> sizer : sizers) {
      totalSize += sizer.applyAsInt(record);
    }

    return totalSize;
  }

  /// Build writer chain for a component - creates type-specific writers at construction time
  BiConsumer<ByteBuffer, Object> buildWriterChain(TypeStructure typeStructure, MethodHandle accessor) {
    LOGGER.finer(() -> "Building writer chain for type structure: " +
        typeStructure.tagTypes().stream().map(t -> t.tag().name()).collect(Collectors.joining("->"))); // TODO revert to FINER logging after bug fix

    // Build the type-specific writer chain
    BiConsumer<ByteBuffer, Object> typeWriter = buildTypeWriterChain(typeStructure);

    // Extract component value with null guard then delegate to type-specific writer
    return (buffer, record) -> {
      try {
        Object componentValue = accessor.invokeWithArguments(record);
        LOGGER.finer(() -> "Extracted component value: " + componentValue + " of type: " +
            (componentValue != null ? componentValue.getClass().getSimpleName() : "null")); // TODO revert to FINER logging after bug fix
        if (componentValue == null) {
          ZigZagEncoding.putInt(buffer, Constants.NULL.ordinal());
          LOGGER.finer(() -> "Writing NULL component at position " + buffer.position()); // TODO revert to FINER logging after bug fix
        } else {
          LOGGER.finer(() -> "Delegating to type writer for value: " + componentValue); // TODO revert to FINER logging after bug fix
          typeWriter.accept(buffer, componentValue);
        }
      } catch (Throwable e) {
        throw new RuntimeException("Failed to write component", e);
      }
    };
  }

  /// Build type-specific writer chain based on TypeStructure
  BiConsumer<ByteBuffer, Object> buildTypeWriterChain(TypeStructure structure) {

    List<TagWithType> tags = structure.tagTypes();

    LOGGER.finer(() -> "Building type writer chain for structure: " +
        tags.stream().map(tagWithType -> tagWithType.tag() != null ? tagWithType.tag().name() : "null").collect(Collectors.joining(","))); // TODO revert to FINER logging after bug fix

    // Reverse the tags to process from right to left
    Iterator<TagWithType> reversedTags = tags.reversed().iterator();
    // Maps require a double look back to get the key and value writers
    List<BiConsumer<ByteBuffer, Object>> writers = new ArrayList<>(tags.size());

    // Start with the leaf (rightmost) writer which cannot be a container type
    TagWithType leafTag = reversedTags.next();
    LOGGER.finer(() -> "Creating leaf writer for rightmost tag: " + leafTag.tag() +
        " with type: " + leafTag.type().getSimpleName());
    BiConsumer<ByteBuffer, Object> writer = createLeafWriter(leafTag);
    writers.add(writer);

    TagWithType priorTag = leafTag;
    // For nested collection or option types we Build chain from right to left (reverse order)
    while (reversedTags.hasNext()) {
      final BiConsumer<ByteBuffer, Object> lastWriter = writer; // final required for lambda capture
      TagWithType nextTag = reversedTags.next();
      LOGGER.finer(() -> "Processing outer tag: " + nextTag.tag() + " with type: " + nextTag.type().getSimpleName());
      writer = switch (nextTag.tag()) {
        case LIST -> createListWriter(lastWriter);
        case OPTIONAL -> createOptionalWriter(lastWriter);
        case MAP -> {
          // As we are going in reverse order we have to flip the last two writers
          final var keyDelegate = writers.getLast();
          final var valueDelegate = writers.get(writers.size() - 2);
          yield createMapWriter(keyDelegate, valueDelegate);
        }
        case ARRAY -> {
          BiConsumer<ByteBuffer, Object> arrayWriter = null;
          switch (priorTag.tag()) {
            // For primitive arrays we can use a direct writer
            case BOOLEAN, BYTE, SHORT, CHARACTER, INTEGER, LONG, FLOAT, DOUBLE, STRING, UUID, ENUM, RECORD ->
                arrayWriter = createDelegatingArrayWriter(priorTag);
            // For optional arrays we need to create a delegating writer
            case OPTIONAL, LIST, MAP, ARRAY -> arrayWriter = createDelegatingArrayWriter(lastWriter);
          }
          assert arrayWriter != null : "Array writer should not be null for tag: " + nextTag.tag();
          yield arrayWriter;
        }
        default -> createLeafWriter(nextTag);
      };
      writers.add(writer);
      priorTag = nextTag; // Update prior tag for next iteration
    }

    LOGGER.finer(() -> "Final writer chain has " + writers.size() + " writers"); // TODO revert to FINER logging after bug fix
    return writer;
  }

  @SuppressWarnings("unused")
  BiConsumer<ByteBuffer, Object> createMapWriter(BiConsumer<ByteBuffer, Object> valueDelegate, BiConsumer<ByteBuffer, Object> delegate) {
    throw new UnsupportedOperationException("Map serialization not yet implemented");
  }

  /// Create leaf writer for primitive/basic types - NO runtime type checking
  BiConsumer<ByteBuffer, Object> createLeafWriter(TagWithType leaftype) {
    LOGGER.fine(() -> "Creating leaf writer for tag: " + leaftype.tag() +
        " with type: " + leaftype.type().getSimpleName());
    return switch (leaftype.tag()) {
      case BOOLEAN -> (buffer, value) -> {
        LOGGER.fine(() -> "Writing BOOLEAN: " + value);
        ZigZagEncoding.putInt(buffer, Constants.BOOLEAN.marker());
        buffer.put(((Boolean) value) ? (byte) 1 : (byte) 0);
      };
      case BYTE -> (buffer, value) -> {
        LOGGER.fine(() -> "Writing BYTE: " + value);
        ZigZagEncoding.putInt(buffer, Constants.BYTE.marker());
        buffer.put((Byte) value);
      };
      case SHORT -> (buffer, value) -> {
        LOGGER.fine(() -> "Writing SHORT: " + value);
        ZigZagEncoding.putInt(buffer, Constants.SHORT.marker());
        buffer.putShort((Short) value);
      };
      case CHARACTER -> (buffer, value) -> {
        LOGGER.fine(() -> "Writing CHARACTER: " + value);
        ZigZagEncoding.putInt(buffer, Constants.CHARACTER.marker());
        buffer.putChar((Character) value);
      };
      case INTEGER -> (buffer, value) -> {
        final Integer intValue = (Integer) value;
        LOGGER.fine(() -> "Writing INTEGER: " + intValue);
        if (ZigZagEncoding.sizeOf(intValue) < Integer.BYTES) {
          ZigZagEncoding.putInt(buffer, -1 * INTEGER_VAR.ordinal());
          ZigZagEncoding.putInt(buffer, intValue);
        } else {
          ZigZagEncoding.putInt(buffer, -1 * INTEGER.ordinal());
          buffer.putInt(intValue);
        }
      };
      case LONG -> (buffer, value) -> {
        final Long longValue = (Long) value;
        LOGGER.fine(() -> "Writing LONG: " + longValue);
        if (ZigZagEncoding.sizeOf(longValue) < Long.BYTES) {
          ZigZagEncoding.putInt(buffer, -1 * LONG_VAR.ordinal());
          ZigZagEncoding.putLong(buffer, longValue);
        } else {
          ZigZagEncoding.putInt(buffer, -1 * LONG.ordinal());
          buffer.putLong(longValue);
        }
      };
      case FLOAT -> (buffer, value) -> {
        LOGGER.fine(() -> "Writing FLOAT: " + value);
        ZigZagEncoding.putInt(buffer, Constants.FLOAT.marker());
        buffer.putFloat((Float) value);
      };
      case DOUBLE -> (buffer, value) -> {
        LOGGER.fine(() -> "Writing DOUBLE: " + value);
        ZigZagEncoding.putInt(buffer, Constants.DOUBLE.marker());
        buffer.putDouble((Double) value);
      };
      case STRING -> (buffer, value) -> {
        LOGGER.fine(() -> "Writing STRING: " + value);
        ZigZagEncoding.putInt(buffer, Constants.STRING.marker());
        byte[] bytes = ((String) value).getBytes(UTF_8);
        ZigZagEncoding.putInt(buffer, bytes.length);
        buffer.put(bytes);
      };
      case UUID -> (buffer, value) -> {
        LOGGER.fine(() -> "Writing UUID: " + value);
        ZigZagEncoding.putInt(buffer, Constants.UUID.marker());
        UUID uuid = (UUID) value;
        buffer.putLong(uuid.getMostSignificantBits());
        buffer.putLong(uuid.getLeastSignificantBits());
      };
      case ENUM -> (buffer, value) -> {
        Enum<?> enumValue = (Enum<?>) value;
        ZigZagEncoding.putInt(buffer, Constants.ENUM.marker());
        Integer ordinal = classToOrdinal.get(enumValue.getClass());
        ZigZagEncoding.putInt(buffer, ordinal + 1);
        String constantName = enumValue.name();
        byte[] constantBytes = constantName.getBytes(UTF_8);
        ZigZagEncoding.putInt(buffer, constantBytes.length);
        buffer.put(constantBytes);
      };
      case RECORD -> (buffer, value) -> {
        LOGGER.fine(() -> "Writing nested RECORD: " + value.getClass().getSimpleName());
        ZigZagEncoding.putInt(buffer, Constants.RECORD.marker());
        // Delegate to this pickler's serialize method for nested records
        if (value instanceof Record record) {
          Integer ordinal = classToOrdinal.get(record.getClass());
          if (ordinal != null) {
            ZigZagEncoding.putInt(buffer, ordinal + 1);
            serializeRecordComponents(buffer, record, ordinal);
          } else {
            throw new IllegalArgumentException("Unknown record type: " + record.getClass());
          }
        }
      };
      default -> throw new IllegalArgumentException("No leaf writer for tag: " + leaftype);
    };
  }

  /// Create array writer that delegates to element writer for generic arrays
  BiConsumer<ByteBuffer, Object> createDelegatingArrayWriter(BiConsumer<ByteBuffer, Object> elementWriter) {
    return (buffer, value) -> {
      LOGGER.fine(() -> "Creating delegating ARRAY with length=" + Array.getLength(value));
      ZigZagEncoding.putInt(buffer, Constants.ARRAY.marker());
      int length = Array.getLength(value);
      ZigZagEncoding.putInt(buffer, length);
      IntStream.range(0, length).forEach(i -> {
        Object element = Array.get(value, i);
        elementWriter.accept(buffer, element);
      });
    };
  }

  /// Create array writer for primitive arrays using tag-based logic
  BiConsumer<ByteBuffer, Object> createDelegatingArrayWriter(final TagWithType typeWithTag) {
    final var outerMarker = Constants.ARRAY.marker();
    LOGGER.finer(() -> "Creating leaf ARRAY writer for element tag: " + typeWithTag.tag() +
        " element type: " + typeWithTag.type().getSimpleName() +
        " outerMarker: " + outerMarker); // TODO revert to FINER logging after bug fix
    return switch (typeWithTag.tag()) {
      case BYTE -> (buffer, value) -> {
        final var bytes = (byte[]) value;
        LOGGER.finer(() -> "Writing BYTE array - position=" + buffer.position() + " value=" + Arrays.toString(bytes)); // TODO revert to FINER logging after bug fix
        LOGGER.finer(() -> "Writing ARRAY outer marker: " + outerMarker); // TODO revert to FINER logging after bug fix
        ZigZagEncoding.putInt(buffer, outerMarker);
        LOGGER.finer(() -> "Writing BYTE element marker: " + (Constants.BYTE.marker())); // TODO revert to FINER logging after bug fix
        ZigZagEncoding.putInt(buffer, Constants.BYTE.marker());
        int length = bytes.length;
        LOGGER.finer(() -> "Writing BYTE array length=" + length); // TODO revert to FINER logging after bug fix
        ZigZagEncoding.putInt(buffer, length);
        LOGGER.finer(() -> "Writing BYTE array data: " + Arrays.toString(bytes)); // TODO revert to FINER logging after bug fix
        buffer.put(bytes);
        LOGGER.finer(() -> "Finished writing BYTE array, final position: " + buffer.position()); // TODO revert to FINER logging after bug fix
      };
      case BOOLEAN -> (buffer, value) -> {
        final var booleans = (boolean[]) value;
        LOGGER.fine(() -> "Writing BOOLEAN array - position=" + buffer.position() + " value=" + Arrays.toString(booleans));
        ZigZagEncoding.putInt(buffer, outerMarker);
        ZigZagEncoding.putInt(buffer, Constants.BOOLEAN.marker());
        int length = booleans.length;
        LOGGER.finer(() -> "Writing BOOLEAN array length=" + length);
        ZigZagEncoding.putInt(buffer, length);
        BitSet bitSet = new BitSet(length);
        // Create a BitSet and flip bits to try where necessary
        IntStream.range(0, length)
            .filter(i -> booleans[i])
            .forEach(bitSet::set);
        byte[] bytes = bitSet.toByteArray();
        LOGGER.finer(() -> "Writing BitSet bytes length=" + bytes.length);
        ZigZagEncoding.putInt(buffer, bytes.length);
        LOGGER.finer(() -> "Writing BitSet bytes in big endian order: " + Arrays.toString(bytes));
        buffer.put(bytes);
      };
      case INTEGER -> (buffer, value) -> {
        final var integers = (int[]) value;
        LOGGER.fine(() -> "Writing INTEGER array - position=" + buffer.position() + " value=" + Arrays.toString(integers));
        final var length = Array.getLength(value);
        final var sampleAverageSize = length > 0 ? estimateAverageSizeInt(integers, length) : 1;
        // Here we must be saving one byte per integer to justify the encoding cost
        if (sampleAverageSize < Integer.BYTES - 1) {
          LOGGER.fine(() -> "Writing INTEGER_VAR array - position=" + buffer.position() + " length=" + length);
          ZigZagEncoding.putInt(buffer, outerMarker);
          ZigZagEncoding.putInt(buffer, -1 * INTEGER_VAR.ordinal());
          ZigZagEncoding.putInt(buffer, length);
          for (int i : integers) {
            ZigZagEncoding.putInt(buffer, i);
          }
        } else {
          LOGGER.fine(() -> "Writing INTEGER array - position=" + buffer.position() + " length=" + length);
          ZigZagEncoding.putInt(buffer, outerMarker);
          ZigZagEncoding.putInt(buffer, Constants.INTEGER.marker());
          ZigZagEncoding.putInt(buffer, length);
          for (int i : integers) {
            buffer.putInt(i);
          }
        }
      };
      case LONG -> (buffer, value) -> {
        final var longs = (long[]) value;
        LOGGER.fine(() -> "Writing LONG array - position=" + buffer.position() + " value=" + Arrays.toString(longs));
        final var length = Array.getLength(value);
        final var sampleAverageSize = length > 0 ? estimateAverageSizeLong(longs, length) : 1;
        // Require 1 byte saving if we sampled the whole array.
        // Require 2 byte saving if we did not sample the whole array as it is large.
        if ((length <= SAMPLE_SIZE && sampleAverageSize < Long.BYTES - 1) ||
            (length > SAMPLE_SIZE && sampleAverageSize < Long.BYTES - 2)) {
          LOGGER.fine(() -> "Writing LONG_VAR array - position=" + buffer.position() + " length=" + length);
          ZigZagEncoding.putInt(buffer, outerMarker);
          ZigZagEncoding.putInt(buffer, -1 * LONG_VAR.ordinal());
          ZigZagEncoding.putInt(buffer, length);
          for (long i : longs) {
            ZigZagEncoding.putLong(buffer, i);
          }
        } else {
          LOGGER.fine(() -> "Writing LONG array - position=" + buffer.position() + " length=" + length);
          ZigZagEncoding.putInt(buffer, outerMarker);
          ZigZagEncoding.putInt(buffer, Constants.LONG.marker());
          ZigZagEncoding.putInt(buffer, length);
          for (long i : longs) {
            buffer.putLong(i);
          }
        }
      };
      case FLOAT -> (buffer, value) -> {
        final var floats = (float[]) value;
        LOGGER.fine(() -> "Writing FLOAT array - position=" + buffer.position() + " value=" + Arrays.toString(floats));
        final var length = Array.getLength(value);
        ZigZagEncoding.putInt(buffer, outerMarker);
        ZigZagEncoding.putInt(buffer, Constants.FLOAT.marker());
        ZigZagEncoding.putInt(buffer, length);
        for (float f : floats) {
          buffer.putFloat(f);
        }
      };
      case DOUBLE -> (buffer, value) -> {
        final var doubles = (double[]) value;
        LOGGER.fine(() -> "Writing DOUBLE array - position=" + buffer.position() + " value=" + Arrays.toString(doubles));
        final var length = Array.getLength(value);
        ZigZagEncoding.putInt(buffer, outerMarker);
        ZigZagEncoding.putInt(buffer, Constants.DOUBLE.marker());
        ZigZagEncoding.putInt(buffer, length);
        for (double d : doubles) {
          buffer.putDouble(d);
        }
      };
      case STRING -> (buffer, value) -> {
        final var strings = (String[]) value;
        LOGGER.fine(() -> "Writing STRING array - position=" + buffer.position() + " value=" + Arrays.toString(strings));
        final var length = Array.getLength(value);
        ZigZagEncoding.putInt(buffer, outerMarker);
        ZigZagEncoding.putInt(buffer, Constants.STRING.marker());
        ZigZagEncoding.putInt(buffer, length);
        for (String s : strings) {
          byte[] bytes = s.getBytes(UTF_8);
          ZigZagEncoding.putInt(buffer, bytes.length);
          buffer.put(bytes);
        }
      };
      case UUID -> (buffer, value) -> {
        final var uuids = (UUID[]) value;
        LOGGER.fine(() -> "Writing UUID array - position=" + buffer.position() + " value=" + Arrays.toString(uuids));
        final var length = Array.getLength(value);
        ZigZagEncoding.putInt(buffer, outerMarker);
        ZigZagEncoding.putInt(buffer, Constants.UUID.marker());
        ZigZagEncoding.putInt(buffer, length);
        for (UUID uuid : uuids) {
          buffer.putLong(uuid.getMostSignificantBits());
          buffer.putLong(uuid.getLeastSignificantBits());
        }
      };
      case SHORT -> (buffer, value) -> {
        final var shorts = (short[]) value;
        LOGGER.fine(() -> "Writing SHORT array - position=" + buffer.position() + " value=" + Arrays.toString(shorts));
        final var length = Array.getLength(value);
        ZigZagEncoding.putInt(buffer, outerMarker);
        ZigZagEncoding.putInt(buffer, Constants.SHORT.marker());
        ZigZagEncoding.putInt(buffer, length);
        for (short s : shorts) {
          buffer.putShort(s);
        }
      };
      case CHARACTER -> (buffer, value) -> {
        final var chars = (char[]) value;
        LOGGER.fine(() -> "Writing CHARACTER array - position=" + buffer.position() + " value=" + Arrays.toString(chars));
        final var length = Array.getLength(value);
        ZigZagEncoding.putInt(buffer, outerMarker);
        ZigZagEncoding.putInt(buffer, Constants.CHARACTER.marker());
        ZigZagEncoding.putInt(buffer, length);
        for (char c : chars) {
          buffer.putChar(c);
        }
      };
      case OPTIONAL -> (buffer, value) -> {
        final var optional = (Optional<?>) value;
        LOGGER.fine(() -> "Writing OPTIONAL array - position=" + buffer.position() + " value=" + optional);
        ZigZagEncoding.putInt(buffer, outerMarker);
        if (optional.isEmpty()) {
          ZigZagEncoding.putInt(buffer, Constants.OPTIONAL_EMPTY.marker());
        } else {
          ZigZagEncoding.putInt(buffer, Constants.OPTIONAL_OF.marker());
          Object innerValue = optional.get();
          createLeafWriter(typeWithTag).accept(buffer, innerValue);
        }
      };
      case RECORD -> (buffer, value) -> {
        final var records = (Record[]) value;
        LOGGER.fine(() -> "Writing RECORD array - position=" + buffer.position() + " length=" + records.length);
        ZigZagEncoding.putInt(buffer, outerMarker);
        ZigZagEncoding.putInt(buffer, Constants.RECORD.marker());
        int length = records.length;
        ZigZagEncoding.putInt(buffer, length);
        // Write component type ordinal using the TagWithType, not runtime lookup
        Integer ordinal = classToOrdinal.get(typeWithTag.type());
        ZigZagEncoding.putInt(buffer, ordinal + 1);
        for (Record record : records) {
          serializeRecordComponents(buffer, record, ordinal);
        }
      };
      case ENUM -> (buffer, value) -> {
        final var enums = (Enum<?>[]) value;
        LOGGER.fine(() -> "Writing ENUM array - position=" + buffer.position() + " value=" + Arrays.toString(enums));
        ZigZagEncoding.putInt(buffer, outerMarker);
        ZigZagEncoding.putInt(buffer, Constants.ENUM.marker());
        int length = enums.length;
        ZigZagEncoding.putInt(buffer, length);
        for (Enum<?> enumValue : enums) {
          Integer ordinal = classToOrdinal.get(enumValue.getClass());
          ZigZagEncoding.putInt(buffer, ordinal + 1);
          String constantName = enumValue.name();
          byte[] constantBytes = constantName.getBytes(UTF_8);
          ZigZagEncoding.putInt(buffer, constantBytes.length);
          buffer.put(constantBytes);
        }
      };
      default -> throw new IllegalArgumentException("Unsupported array type for direct writer: " + typeWithTag.tag());
    };
  }

  /// Helper methods for array sizing

  static int estimateAverageSizeLong(long[] longs, int length) {
    final var sampleLength = Math.min(length, SAMPLE_SIZE);
    final var sampleSize = IntStream.range(0, sampleLength)
        .map(i -> ZigZagEncoding.sizeOf(longs[i]))
        .sum();
    return sampleSize / sampleLength;
  }

  static int estimateAverageSizeInt(int[] integers, int length) {
    final var sampleLength = Math.min(length, SAMPLE_SIZE);
    final var sampleSize = IntStream.range(0, sampleLength)
        .map(i -> ZigZagEncoding.sizeOf(integers[i]))
        .sum();
    return sampleSize / sampleLength;
  }

  /// Create writer for Optional values
  BiConsumer<ByteBuffer, Object> createOptionalWriter(BiConsumer<ByteBuffer, Object> elementWriter) {
    return (buffer, value) -> {
      Optional<?> optional = (Optional<?>) value;
      if (optional.isEmpty()) {
        LOGGER.fine(() -> "Writing OPTIONAL_EMPTY");
        ZigZagEncoding.putInt(buffer, Constants.OPTIONAL_EMPTY.marker());
      } else {
        LOGGER.fine(() -> "Writing OPTIONAL_OF");
        ZigZagEncoding.putInt(buffer, Constants.OPTIONAL_OF.marker());
        elementWriter.accept(buffer, optional.get());
      }
    };
  }

  /// Create writer for List values
  BiConsumer<ByteBuffer, Object> createListWriter(BiConsumer<ByteBuffer, Object> elementWriter) {
    return (buffer, value) -> {
      List<?> list = (List<?>) value;
      LOGGER.fine(() -> "Writing LIST size=" + list.size());
      ZigZagEncoding.putInt(buffer, Constants.LIST.marker());
      buffer.putInt(list.size());
      for (Object item : list) {
        elementWriter.accept(buffer, item);
      }
    };
  }

  /// Build reader chain for a component
  /// Use `assert` to check that the read back type match what we expected
  Function<ByteBuffer, Object> buildReaderChain(TypeStructure typeStructure) {
    // Build the type-specific reader chain
    Function<ByteBuffer, Object> typeReader = buildTypeReaderChain(typeStructure);

    // Add null guard - check for NULL marker first
    return buffer -> {
      buffer.mark();
      int marker = ZigZagEncoding.getInt(buffer);
      if (marker == 0) {
        LOGGER.fine(() -> "Reading NULL component");
        return null;
      } else {
        buffer.reset(); // Reset to before marker
        return typeReader.apply(buffer);
      }
    };
  }

  /// Build type-specific reader chain based on TypeStructure
  Function<ByteBuffer, Object> buildTypeReaderChain(TypeStructure structure) {
    List<TagWithType> tags = structure.tagTypes();

    // For complex types, build chain from right to left
    Iterator<TagWithType> reversedTags = tags.reversed().iterator();
    // TODO Maps require a double look back to get the key and value writers
    List<Function<ByteBuffer, Object>> readers = new ArrayList<>(tags.size());

    TagWithType leafTag = reversedTags.next();
    Function<ByteBuffer, Object> reader = createLeafReader(leafTag);

    TagWithType priorTag = leafTag;
    while (reversedTags.hasNext()) {
      final Function<ByteBuffer, Object> innerReader = reader;
      TagWithType tag = reversedTags.next();
      reader = switch (tag.tag()) {
        case LIST -> createListReader(innerReader);
        case OPTIONAL -> createOptionalReader(innerReader);
        case MAP -> throw new AssertionError("Map deserialization not yet implemented");
        case ARRAY -> {
          Function<ByteBuffer, Object> arrayReader = null;
          switch (priorTag.tag()) {
            // For primitive arrays we can use a direct reader
            case BOOLEAN, BYTE, SHORT, CHARACTER, INTEGER, LONG, FLOAT, DOUBLE, STRING, UUID, ENUM, RECORD ->
                arrayReader = createLeafArrayReader(priorTag);
            // For optional arrays we need to create a delegating reader
            case OPTIONAL, LIST, MAP, ARRAY -> arrayReader = createDelegatingArrayReader(innerReader);
          }
          yield arrayReader;
        }
        default -> createLeafReader(tag);
      };
      readers.add(reader);
      priorTag = tag; // Update prior tag for next iteration
    }

    return reader;
  }

  Function<ByteBuffer, Object> createDelegatingArrayReader(Function<ByteBuffer, Object> innerReader) {
    throw new AssertionError("not yet implemented");
  }

  Function<ByteBuffer, Object> createLeafArrayReader(TagWithType priorTag) {
    Function<ByteBuffer, Object> reader = switch (priorTag.tag()) {
      case BOOLEAN -> (buffer) -> {
        int boolLength = ZigZagEncoding.getInt(buffer);
        boolean[] booleans = new boolean[boolLength];
        int bytesLength = ZigZagEncoding.getInt(buffer);
        byte[] bytes = new byte[bytesLength];
        buffer.get(bytes);
        BitSet bitSet = BitSet.valueOf(bytes);
        IntStream.range(0, boolLength).forEach(i -> booleans[i] = bitSet.get(i));
        return booleans;
      };
      case BYTE -> (buffer) -> {
        int length = ZigZagEncoding.getInt(buffer);
        byte[] bytes = new byte[length];
        buffer.get(bytes);
        return bytes;
      };
      case SHORT -> (buffer) -> {
        int length = ZigZagEncoding.getInt(buffer);
        short[] shorts = new short[length];
        IntStream.range(0, length).forEach(i -> shorts[i] = buffer.getShort());
        return shorts;
      };
      case CHARACTER -> (buffer) -> {
        int length = ZigZagEncoding.getInt(buffer);
        char[] chars = new char[length];
        IntStream.range(0, length).forEach(i -> chars[i] = buffer.getChar());
        return chars;
      };
      case INTEGER -> (buffer) -> {
        // Check if we have a variable-length integer array
        int priorTagValue = ZigZagEncoding.getInt(buffer);
        if (priorTagValue == Constants.INTEGER_VAR.marker()) {
          int length = ZigZagEncoding.getInt(buffer);
          int[] integers = new int[length];
          IntStream.range(0, length).forEach(i -> integers[i] = ZigZagEncoding.getInt(buffer));
          return integers;
        } else if (priorTagValue == Constants.INTEGER.marker()) {
          // Fixed-length integer array
          int length = ZigZagEncoding.getInt(buffer);
          int[] integers = new int[length];
          IntStream.range(0, length).forEach(i -> integers[i] = buffer.getInt());
          return integers;
        } else throw new IllegalStateException("Expected INTEGER or INTEGER_VAR marker but got: " + priorTagValue);
      };
      case LONG -> (buffer) -> {
        // Check if we have a variable-length long array
        int priorTagValue = ZigZagEncoding.getInt(buffer);
        if (priorTagValue == Constants.LONG_VAR.marker()) {
          int length = ZigZagEncoding.getInt(buffer);
          long[] longs = new long[length];
          IntStream.range(0, length).forEach(i -> longs[i] = ZigZagEncoding.getLong(buffer));
          return longs;
        } else if (priorTagValue == Constants.LONG.marker()) {
          // Fixed-length long array
          int length = ZigZagEncoding.getInt(buffer);
          long[] longs = new long[length];
          IntStream.range(0, length).forEach(i -> longs[i] = buffer.getLong());
          return longs;
        } else throw new IllegalStateException("Expected LONG or LONG_VAR marker but got: " + priorTagValue);
      };
      case FLOAT -> (buffer) -> {
        int length = ZigZagEncoding.getInt(buffer);
        float[] floats = new float[length];
        IntStream.range(0, length).forEach(i -> floats[i] = buffer.getFloat());
        return floats;
      };
      case DOUBLE -> (buffer) -> {
        int length = ZigZagEncoding.getInt(buffer);
        double[] doubles = new double[length];
        IntStream.range(0, length).forEach(i -> doubles[i] = buffer.getDouble());
        return doubles;
      };
      case STRING -> (buffer) -> {
        int length = ZigZagEncoding.getInt(buffer);
        String[] strings = new String[length];
        IntStream.range(0, length).forEach(i -> {
          int strLength = ZigZagEncoding.getInt(buffer);
          byte[] bytes = new byte[strLength];
          buffer.get(bytes);
          strings[i] = new String(bytes, UTF_8);
        });
        return strings;
      };
      case UUID -> (buffer) -> {
        int length = ZigZagEncoding.getInt(buffer);
        UUID[] uuids = new UUID[length];
        IntStream.range(0, length).forEach(i -> {
          long mostSigBits = buffer.getLong();
          long leastSigBits = buffer.getLong();
          uuids[i] = new UUID(mostSigBits, leastSigBits);
        });
        return uuids;
      };
      case RECORD -> (buffer) -> {
        int length = ZigZagEncoding.getInt(buffer);
        int componentWireOrdinal = ZigZagEncoding.getInt(buffer);
        int componentOrdinal = componentWireOrdinal - 1;
        Class<?> componentType = userTypes[componentOrdinal];
        Record[] records = (Record[]) Array.newInstance(componentType, length);
        Arrays.setAll(records, i -> deserializeRecord(buffer, componentOrdinal));
        return records;
      };
      case ENUM -> (buffer) -> {
        int length = ZigZagEncoding.getInt(buffer);
        Enum<?>[] enums = (Enum<?>[]) Array.newInstance(userTypes[0], length); // Assuming all enums are of the same type
        IntStream.range(0, length).forEach(i -> {
          int wireOrdinal = ZigZagEncoding.getInt(buffer);
          int ordinal = wireOrdinal - 1;
          Class<?> enumClass = userTypes[ordinal];
          int constantLength = ZigZagEncoding.getInt(buffer);
          byte[] constantBytes = new byte[constantLength];
          buffer.get(constantBytes);
          String constantName = new String(constantBytes, UTF_8);
          //noinspection unchecked
          enums[i] = Enum.valueOf((Class<Enum>) enumClass, constantName);
        });
        return enums;
      };
      default -> throw new IllegalStateException("Unsupported array type marker: " + priorTag);
    };
    return buffer -> {
      int marker = ZigZagEncoding.getInt(buffer);
      LOGGER.finer(() -> "Read ARRAY marker: " + marker + " expected: " + (Constants.ARRAY.marker()) + " at position " + buffer.position());
      assert (marker != Constants.ARRAY.marker()): "Expected ARRAY marker but got: " + marker;
      return reader.apply(buffer);
    };
  }

  /// Create leaf reader for primitive/basic types
  Function<ByteBuffer, Object> createLeafReader(TagWithType leafTag) {
    return switch (leafTag.tag()) {
      case BOOLEAN -> buffer -> {
        int marker = ZigZagEncoding.getInt(buffer);
        if (marker != Constants.BOOLEAN.marker()) {
          throw new IllegalStateException("Expected BOOLEAN marker but got: " + marker);
        }
        boolean value = buffer.get() != 0;
        LOGGER.fine(() -> "Read Boolean: " + value);
        return value;
      };
      case BYTE -> buffer -> {
        int marker = ZigZagEncoding.getInt(buffer);
        if (marker != Constants.BYTE.marker()) {
          throw new IllegalStateException("Expected BYTE marker but got: " + marker);
        }
        byte value = buffer.get();
        LOGGER.fine(() -> "Read Byte: " + value);
        return value;
      };
      case SHORT -> buffer -> {
        int marker = ZigZagEncoding.getInt(buffer);
        if (marker != Constants.SHORT.marker()) {
          throw new IllegalStateException("Expected SHORT marker but got: " + marker);
        }
        short value = buffer.getShort();
        LOGGER.fine(() -> "Read Short: " + value);
        return value;
      };
      case CHARACTER -> buffer -> {
        int marker = ZigZagEncoding.getInt(buffer);
        if (marker != Constants.CHARACTER.marker()) {
          throw new IllegalStateException("Expected CHARACTER marker but got: " + marker);
        }
        char value = buffer.getChar();
        LOGGER.fine(() -> "Read Character: " + value);
        return value;
      };
      case INTEGER -> buffer -> {
        int marker = ZigZagEncoding.getInt(buffer);
        if (marker == -1 * INTEGER_VAR.ordinal()) {
          int value = ZigZagEncoding.getInt(buffer);
          LOGGER.fine(() -> "Read Integer (ZigZag): " + value);
          return value;
        } else if (marker == -1 * INTEGER.ordinal()) {
          int value = buffer.getInt();
          LOGGER.fine(() -> "Read Integer: " + value);
          return value;
        } else {
          throw new IllegalStateException("Expected INTEGER marker but got: " + marker);
        }
      };
      case LONG -> buffer -> {
        int marker = ZigZagEncoding.getInt(buffer);
        if (marker == -1 * LONG_VAR.ordinal()) {
          long value = ZigZagEncoding.getLong(buffer);
          LOGGER.fine(() -> "Read Long (ZigZag): " + value);
          return value;
        } else if (marker == -1 * LONG.ordinal()) {
          long value = buffer.getLong();
          LOGGER.fine(() -> "Read Long: " + value);
          return value;
        } else {
          throw new IllegalStateException("Expected LONG marker but got: " + marker);
        }
      };
      case FLOAT -> buffer -> {
        int marker = ZigZagEncoding.getInt(buffer);
        if (marker != Constants.FLOAT.marker()) {
          throw new IllegalStateException("Expected FLOAT marker but got: " + marker);
        }
        float value = buffer.getFloat();
        LOGGER.fine(() -> "Read Float: " + value);
        return value;
      };
      case DOUBLE -> buffer -> {
        int marker = ZigZagEncoding.getInt(buffer);
        if (marker != Constants.DOUBLE.marker()) {
          throw new IllegalStateException("Expected DOUBLE marker but got: " + marker);
        }
        double value = buffer.getDouble();
        LOGGER.fine(() -> "Read Double: " + value);
        return value;
      };
      case STRING -> buffer -> {
        int marker = ZigZagEncoding.getInt(buffer);
        if (marker != Constants.STRING.marker()) {
          throw new IllegalStateException("Expected STRING marker but got: " + marker);
        }
        int length = ZigZagEncoding.getInt(buffer);
        byte[] bytes = new byte[length];
        buffer.get(bytes);
        String value = new String(bytes, UTF_8);
        LOGGER.fine(() -> "Read String: " + value);
        return value;
      };
      case UUID -> buffer -> {
        int marker = ZigZagEncoding.getInt(buffer);
        if (marker != Constants.UUID.marker()) {
          throw new IllegalStateException("Expected UUID marker but got: " + marker);
        }
        final long mostSigBits = buffer.getLong();
        final long leastSigBits = buffer.getLong();
        UUID value = new UUID(mostSigBits, leastSigBits);
        LOGGER.fine(() -> "Read UUID: " + value);
        return value;
      };
      case ENUM -> buffer -> {
        int marker = ZigZagEncoding.getInt(buffer);
        int wireOrdinal = ZigZagEncoding.getInt(buffer);
        int ordinal = wireOrdinal - 1;
        Class<?> enumClass = userTypes[ordinal];

        int constantLength = ZigZagEncoding.getInt(buffer);
        byte[] constantBytes = new byte[constantLength];
        buffer.get(constantBytes);
        String constantName = new String(constantBytes, UTF_8);

        @SuppressWarnings({"unchecked", "rawtypes"})
        Enum<?> value = Enum.valueOf((Class<? extends Enum>) enumClass, constantName);
        return value;
      };
      case RECORD -> buffer -> {
        int marker = ZigZagEncoding.getInt(buffer);
        if (marker != Constants.RECORD.marker()) {
          throw new IllegalStateException("Expected RECORD marker but got: " + marker);
        }

        // Read ordinal (1-indexed on wire)
        int wireOrdinal = ZigZagEncoding.getInt(buffer);
        int ordinal = wireOrdinal - 1;

        if (ordinal < 0 || ordinal >= userTypes.length) {
          throw new IllegalStateException("Invalid record ordinal: " + ordinal);
        }

        return deserializeRecord(buffer, ordinal);
      };
      default -> throw new IllegalArgumentException("No leaf reader for tag: " + leafTag);
    };
  }

  /// Create reader for Optional values
  Function<ByteBuffer, Object> createOptionalReader(Function<ByteBuffer, Object> elementReader) {
    return buffer -> {
      int marker = ZigZagEncoding.getInt(buffer);
      if (marker == Constants.OPTIONAL_EMPTY.marker()) {
        LOGGER.fine(() -> "Reading OPTIONAL_EMPTY");
        return Optional.empty();
      } else if (marker == Constants.OPTIONAL_OF.marker()) {
        Object value = elementReader.apply(buffer);
        LOGGER.fine(() -> "Reading OPTIONAL_OF with value: " + value);
        return Optional.of(value);
      } else {
        throw new IllegalStateException("Expected OPTIONAL marker but got: " + marker);
      }
    };
  }

  /// Create reader for List values
  Function<ByteBuffer, Object> createListReader(Function<ByteBuffer, Object> elementReader) {
    return buffer -> {
      int marker = ZigZagEncoding.getInt(buffer);
      if (marker != Constants.LIST.marker()) {
        throw new IllegalStateException("Expected LIST marker but got: " + marker);
      }
      int size = buffer.getInt();
      LOGGER.fine(() -> "Reading List with " + size + " elements");
      List<Object> list = new ArrayList<>(size);
      for (int i = 0; i < size; i++) {
        list.add(elementReader.apply(buffer));
      }
      return Collections.unmodifiableList(list);
    };
  }

  /// Build sizer chain for a component - creates type-specific sizers at construction time
  ToIntFunction<Object> buildSizerChain(TypeStructure typeStructure, MethodHandle accessor) {
    // Build the type-specific sizer
    ToIntFunction<Object> typeSizer = buildTypeSizerChain(typeStructure);

    // Extract component value then delegate to type-specific sizer
    return record -> {
      try {
        Object componentValue = accessor.invokeWithArguments(record);
        return componentValue == null ? 1 : typeSizer.applyAsInt(componentValue);
      } catch (Throwable e) {
        throw new RuntimeException("Failed to size component", e);
      }
    };
  }

  /// Build type-specific sizer chain based on TypeStructure
  ToIntFunction<Object> buildTypeSizerChain(TypeStructure structure) {
    List<TagWithType> tags = structure.tagTypes();
    if (tags.isEmpty()) {
      throw new IllegalArgumentException("Type structure must have at least one tag");
    }

    // For complex types, build chain from right to left
    Iterator<TagWithType> reversedTags = tags.reversed().iterator();
    TagWithType rightmostTag = reversedTags.next();
    ToIntFunction<Object> sizer = createLeafSizer(rightmostTag);

    while (reversedTags.hasNext()) {
      final ToIntFunction<Object> innerSizer = sizer;
      TagWithType tag = reversedTags.next();
      sizer = switch (tag.tag()) {
        case LIST -> createListSizer(innerSizer);
        case OPTIONAL -> createOptionalSizer(innerSizer);
        case ARRAY -> createArraySizer(innerSizer);
        default -> createLeafSizer(tag);
      };
    }

    return sizer;
  }

  /// Create leaf sizer for primitive/basic types
  ToIntFunction<Object> createLeafSizer(TagWithType leafTag) {
    return switch (leafTag.tag()) {
      case BOOLEAN -> obj -> 1 + 1; // marker + boolean byte
      case BYTE -> obj -> 1 + 1; // marker + byte
      case SHORT -> obj -> 1 + Short.BYTES;
      case CHARACTER -> obj -> 1 + Character.BYTES;
      case INTEGER -> obj -> {
        Integer value = (Integer) obj;
        int zigzagSize = ZigZagEncoding.sizeOf(value);
        return 1 + (Math.min(zigzagSize, Integer.BYTES));
      };
      case LONG -> obj -> {
        Long value = (Long) obj;
        int zigzagSize = ZigZagEncoding.sizeOf(value);
        return 1 + (Math.min(zigzagSize, Long.BYTES));
      };
      case FLOAT -> obj -> 1 + Float.BYTES;
      case DOUBLE -> obj -> 1 + Double.BYTES;
      case STRING -> obj -> {
        String s = (String) obj;
        byte[] bytes = s.getBytes(UTF_8);
        return 1 + ZigZagEncoding.sizeOf(bytes.length) + bytes.length;
      };
      case UUID -> obj -> 1 + 2 * Long.BYTES;
      case ARRAY -> value -> {
        if (value == null) {
          return 1; // NULL marker size
        }
        return 1 + switch (value) {
          case byte[] arr -> 1 + 4 + arr.length; // marker + type + length + data
          case boolean[] booleans -> {
            int bitSetBytes = (booleans.length + 7) / 8;
            yield 1 + 4 + 4 + bitSetBytes; // marker + type + length + bitset length + bitset
          }
          case int[] integers -> 1 + 4 + integers.length * Integer.BYTES; // worst case
          case long[] longs -> 1 + 4 + longs.length * Long.BYTES; // worst case
          case float[] floats -> 1 + 4 + floats.length * Float.BYTES;
          case double[] doubles -> 1 + 4 + doubles.length * Double.BYTES;
          case short[] shorts -> 1 + 4 + shorts.length * Short.BYTES;
          case char[] chars -> 1 + 4 + chars.length * Character.BYTES;
          case String[] strings -> {
            int overhead = 1 + 4; // component type + length
            int contentSize = Arrays.stream(strings).mapToInt(s -> ((ToIntFunction<Object>) obj -> {
              String s1 = (String) obj;
              byte[] bytes = s1.getBytes(UTF_8);
              return 1 + ZigZagEncoding.sizeOf(bytes.length) + bytes.length;
            }).applyAsInt(s) - 1).sum();
            yield overhead + contentSize;
          }
          case UUID[] uuids -> 1 + 4 + uuids.length * (2 * Long.BYTES);
          default -> throw new AssertionError("not implemented for array type: " + value.getClass());
        };
      };
      case ENUM -> obj -> {
        Enum<?> enumValue = (Enum<?>) obj;
        Integer ordinal = classToOrdinal.get(enumValue.getClass());
        String constantName = enumValue.name();
        byte[] nameBytes = constantName.getBytes(UTF_8);
        return 1 + ZigZagEncoding.sizeOf(ordinal + 1) + ZigZagEncoding.sizeOf(nameBytes.length) + nameBytes.length;
      };
      case RECORD -> obj -> {
        Record record = (Record) obj;
        Integer ordinal = classToOrdinal.get(record.getClass());
        if (ordinal != null) {
          return ZigZagEncoding.sizeOf(ordinal + 1) + maxSizeOfRecordComponents(record, ordinal);
        } else {
          throw new IllegalArgumentException("Unknown record type: " + record.getClass());
        }
      };
      default -> throw new IllegalArgumentException("No leaf sizer for tag: " + leafTag);
    };
  }

  /// Create sizer for Optional values
  ToIntFunction<Object> createOptionalSizer
  (ToIntFunction<Object> elementSizer) {
    return obj -> {
      Optional<?> optional = (Optional<?>) obj;
      // OPTIONAL_EMPTY marker
      // OPTIONAL_OF marker + element
      return optional.map(o -> 1 + elementSizer.applyAsInt(o)).orElse(1);
    };
  }

  /// Create sizer for List values
  ToIntFunction<Object> createListSizer(ToIntFunction<Object> elementSizer) {
    return obj -> {
      List<?> list = (List<?>) obj;
      int size = 1 + 4; // LIST marker + size int
      return size + list.stream().mapToInt(elementSizer).sum();
    };
  }

  /// Create sizer for Array values with element delegation
  ToIntFunction<Object> createArraySizer(ToIntFunction<Object> elementSizer) {
    return obj -> {
      int length = Array.getLength(obj);
      int size = 1 + 1 + ZigZagEncoding.sizeOf(length); // ARRAY marker + element type marker + length
      return size + IntStream.range(0, length)
          .mapToObj(i -> Array.get(obj, i))
          .mapToInt(elementSizer)
          .sum();
    };
  }

  @Override
  public int serialize(ByteBuffer buffer, T object) {
    Objects.requireNonNull(buffer, "buffer cannot be null");
    Objects.requireNonNull(object, "object cannot be null");
    buffer.order(ByteOrder.BIG_ENDIAN);
    final var startPosition = buffer.position();

    // Find ordinal for the root object's class using the ONE HashMap lookup
    final Integer ordinalObj = classToOrdinal.get(object.getClass());
    if (ordinalObj == null) {
      throw new IllegalArgumentException("Unknown class: " + object.getClass().getName());
    }
    final int ordinal = ordinalObj;
    LOGGER.finer(() -> "Serializing ordinal " + ordinal + " (" + object.getClass().getSimpleName() + ")");

    // Write ordinal marker (1-indexed on wire)
    ZigZagEncoding.putInt(buffer, ordinal + 1);

    // Serialize record components using pre-built writers array (NO HashMap lookups on hot path)
    if (object instanceof Record record) {
      serializeRecordComponents(buffer, record, ordinal);
    } else {
      throw new IllegalArgumentException("Expected Record but got: " + object.getClass().getName());
    }

    final var totalBytes = buffer.position() - startPosition;
    LOGGER.finer(() -> "PicklerImpl.serialize: completed, totalBytes=" + totalBytes);
    return totalBytes;
  }


  @Override
  public T deserialize(ByteBuffer buffer) {
    Objects.requireNonNull(buffer, "buffer cannot be null");
    buffer.order(ByteOrder.BIG_ENDIAN);
    LOGGER.finer(() -> "PicklerImpl.deserialize: starting at position=" + buffer.position());

    // Read ordinal marker (1-indexed on wire, convert to 0-indexed for array access)
    final int wireOrdinal = ZigZagEncoding.getInt(buffer);
    final int ordinal = wireOrdinal - 1;

    LOGGER.finer(() -> "Deserializing ordinal " + ordinal + " (wire=" + wireOrdinal + ")");

    if (ordinal < 0 || ordinal >= userTypes.length) {
      throw new IllegalStateException("Invalid ordinal: " + ordinal + " (wire=" + wireOrdinal + ")");
    }

    Class<?> targetClass = userTypes[ordinal];
    LOGGER.finer(() -> "Target class: " + targetClass.getSimpleName());

    // Deserialize using pre-built component readers and constructor (direct array access)
    if (targetClass.isRecord()) {
      return deserializeRecord(buffer, ordinal);
    } else {
      throw new IllegalArgumentException("Expected Record class but got: " + targetClass.getName());
    }
  }

  @Override
  public int maxSizeOf(T object) {
    Objects.requireNonNull(object, "object cannot be null");

    // Find ordinal for the object's class using the ONE HashMap lookup
    final Integer ordinalObj = classToOrdinal.get(object.getClass());
    if (ordinalObj == null) {
      throw new IllegalArgumentException("Unknown class: " + object.getClass().getName());
    }
    final int ordinal = ordinalObj;

    // Size = ordinal marker + record component sizes (using pre-built sizers array)
    int size = ZigZagEncoding.sizeOf(ordinal + 1); // 1-indexed on wire

    if (object instanceof Record record) {
      size += maxSizeOfRecordComponents(record, ordinal);
    }

    return size;
  }

  /// Discover all reachable types from a root class including sealed hierarchies and record components
  static Stream<Class<?>> recordClassHierarchy(final Class<?> current, final Set<Class<?>> visited) {
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
                .flatMap(component -> {
                  LOGGER.finer(() -> "Analyzing component " + component.getName() + " with type " + component.getGenericType());
                  try {
                    TypeStructure structure = TypeStructure.analyze(component.getGenericType());
                    LOGGER.finer(() -> "Component " + component.getName() + " discovered types: " +
                        structure.tagTypes().stream().map(TagWithType::type).map(Class::getSimpleName).toList());
                    return structure.tagTypes().stream().map(TagWithType::type);
                  } catch (Exception e) {
                    LOGGER.finer(() -> "Failed to analyze component " + component.getName() + ": " + e.getMessage());
                    return Stream.of(component.getType()); // Fallback to direct type
                  }
                })
                .filter(t -> t.isRecord() || t.isSealed() || t.isEnum())
                : Stream.empty()
        ).flatMap(child -> recordClassHierarchy(child, visited))
    );
  }


}

record TagWithType(Tag tag, Class<?> type) {
}

/// TypeStructure record for analyzing generic types
record TypeStructure(List<TagWithType> tagTypes) {

  TypeStructure(List<Tag> tags, List<Class<?>> types) {
    this(IntStream.range(0, Math.min(tags.size(), types.size()))
        .mapToObj(i -> new TagWithType(tags.get(i), types.get(i)))
        .toList());
    if (tags.size() != types.size()) {
      throw new IllegalArgumentException("Tags and types lists must have same size: tags=" + tags.size() + " types=" + types.size() +
          " tags=" + tags + " types=" + types);
    }
  }

  /// Analyze a generic type and extract its structure
  static TypeStructure analyze(Type type) {
    List<Tag> tags = new ArrayList<>();
    List<Class<?>> types = new ArrayList<>();

    Object current = type;

    while (current != null) {
      if (current instanceof ParameterizedType paramType) {
        Type rawType = paramType.getRawType();

        if (rawType.equals(List.class)) {
          tags.add(LIST);
          types.add(List.class); // Container class for symmetry with Arrays.class pattern
          Type[] typeArgs = paramType.getActualTypeArguments();
          current = typeArgs.length > 0 ? typeArgs[0] : null;
        } else if (rawType.equals(Map.class)) {
          tags.add(MAP);
          types.add(Map.class); // Container class for symmetry  
          // For maps, we need to handle both key and value types, but for simplicity we'll skip for now
          return new TypeStructure(tags, types);
        } else if (rawType.equals(Optional.class)) {
          tags.add(OPTIONAL);
          types.add(Optional.class); // Container class for symmetry
          Type[] typeArgs = paramType.getActualTypeArguments();
          current = typeArgs.length > 0 ? typeArgs[0] : null;
        } else {
          // Unknown parameterized type, treat as raw type
          current = rawType;
        }
      } else if (current instanceof GenericArrayType arrayType) {
        // Handle arrays of parameterized types like Optional<String>[]
        tags.add(ARRAY);
        types.add(Arrays.class); // Arrays.class as marker
        Type componentType = arrayType.getGenericComponentType();
        current = componentType; // Continue processing element type
      } else if (current instanceof Class<?> clazz) {
        if (clazz.isArray()) {
          // Array container with element type, e.g. short[] -> [ARRAY, SHORT]
          tags.add(ARRAY);
          types.add(Arrays.class); // Arrays.class as marker, not concrete array type
          Type componentType = clazz.getComponentType();
          current = componentType; // Continue processing element type
        } else {
          // Regular class - terminal case
          tags.add(fromClass(clazz));
          types.add(clazz);
          return new TypeStructure(tags, types);
        }
      } else {
        // Unknown type, return what we have
        return new TypeStructure(tags, types);
      }
    }

    return new TypeStructure(tags, types);
  }
}
