// SPDX-FileCopyrightText: 2025 Simon Massey
// SPDX-License-Identifier: MIT

package io.github.simbo1905.no.framework;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Array;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.RecordComponent;
import java.lang.reflect.Type;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.ToIntFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static io.github.simbo1905.no.framework.Tag.*;
import static java.nio.charset.StandardCharsets.UTF_8;

/// Unified pickler implementation that handles all reachable types using array-based architecture.
/// Eliminates the need for separate RecordPickler and SealedPickler classes.
final class PicklerImpl<T> implements Pickler<T> {

  // Static final array for O(1) Constants lookup without defensive copying on hot path
  static final Constants[] CONSTANTS_ARRAY = Constants.values();
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

    // Build metadata for each discovered record type using meta-programming
    for (int ordinal = 0; ordinal < numRecordTypes; ordinal++) { // TODO no for loop, use streams
      Class<?> recordClass = userTypes[ordinal];
      if (recordClass.isRecord()) {
        metaprogramming(ordinal, recordClass);
      }
    }

    LOGGER.info(() -> "PicklerImpl construction complete - ready for high-performance serialization");
  }

  /// Build component metadata for a record type using meta-programming
  @SuppressWarnings("unchecked")
  void metaprogramming(int ordinal, Class<?> recordClass) {
    LOGGER.fine(() -> "Building metadata for ordinal " + ordinal + ": " + recordClass.getSimpleName());

    // Get record constructor
    recordConstructors[ordinal] = getRecordConstructor(recordClass);

    // Get component accessors and analyze types
    RecordComponent[] components = recordClass.getRecordComponents();
    int numComponents = components.length;

    componentAccessors[ordinal] = new MethodHandle[numComponents];
    componentTypes[ordinal] = new TypeStructure[numComponents];
    componentWriters[ordinal] = new BiConsumer[numComponents];
    componentReaders[ordinal] = new Function[numComponents];
    componentSizers[ordinal] = new ToIntFunction[numComponents];

    for (int i = 0; i < numComponents; i++) { // TODO no for loop, use streams
      RecordComponent component = components[i];

      // Get accessor method handle
      try {
        componentAccessors[ordinal][i] = MethodHandles.lookup().unreflect(component.getAccessor());
      } catch (IllegalAccessException e) {
        throw new RuntimeException("Failed to un reflect accessor for " + component.getName(), e);
      }

      // Static analysis of component type structure of the component to understand if it has nested containers
      // like array, list, map, optional,  etc
      componentTypes[ordinal][i] = TypeStructure.analyze(component.getGenericType());

      // Build writer, reader, and sizer chains (simplified for now)
      componentWriters[ordinal][i] = buildWriterChain(componentTypes[ordinal][i], componentAccessors[ordinal][i]);
      componentReaders[ordinal][i] = buildReaderChain(componentTypes[ordinal][i]);
      componentSizers[ordinal][i] = buildSizerChain(componentTypes[ordinal][i], componentAccessors[ordinal][i]);
    }

    LOGGER.fine(() -> "Completed metadata for " + recordClass.getSimpleName() + " with " + numComponents + " components");
  }

  /// Serialize record components using pre-built writers array (NO HashMap lookups)
  void serializeRecordComponents(ByteBuffer buffer, Record record, int ordinal) {
    BiConsumer<ByteBuffer, Object>[] writers = componentWriters[ordinal];
    if (writers == null) {
      throw new IllegalStateException("No writers for ordinal: " + ordinal);
    }

    LOGGER.finer(() -> "*** DEBUG: Serializing " + writers.length + " components for " + record.getClass().getSimpleName() + 
        " at position " + buffer.position()); // TODO revert to FINER logging after bug fix

    // Use pre-built writers - direct array access, no HashMap lookups
    for (int i = 0; i < writers.length; i++) {
      final int componentIndex = i; // final for lambda capture
      LOGGER.finer(() -> "*** DEBUG: Writing component " + componentIndex + " at position " + buffer.position()); // TODO revert to FINER logging after bug fix
      writers[i].accept(buffer, record);
      LOGGER.finer(() -> "*** DEBUG: Finished component " + componentIndex + " at position " + buffer.position()); // TODO revert to FINER logging after bug fix
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

    LOGGER.finer(() -> "*** DEBUG: Deserializing " + readers.length + " components at position " + buffer.position()); // TODO revert to FINER logging after bug fix

    // Read components using pre-built readers
    Object[] components = new Object[readers.length];
    for (int i = 0; i < readers.length; i++) {
      final int componentIndex = i; // final for lambda capture
      LOGGER.finer(() -> "*** DEBUG: Reading component " + componentIndex + " at position " + buffer.position()); // TODO revert to FINER logging after bug fix
      components[i] = readers[i].apply(buffer);
      final Object componentValue = components[i]; // final for lambda capture
      LOGGER.finer(() -> "*** DEBUG: Read component " + componentIndex + ": " + componentValue + " at position " + buffer.position()); // TODO revert to FINER logging after bug fix
    }

    // Invoke constructor
    try {
      LOGGER.finer(() -> "*** DEBUG: Constructing record with components: " + Arrays.toString(components)); // TODO revert to FINER logging after bug fix
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
    if (tags.isEmpty()) { // TODO pull this up to the call site
      throw new IllegalArgumentException("Type structure must have at least one tag");
    }
    LOGGER.finer(() -> "*** DEBUG: Building type writer chain for structure: " +
        tags.stream().map(tagWithType -> tagWithType.tag() != null ? tagWithType.tag().name() : "null").collect(Collectors.joining(","))); // TODO revert to FINER logging after bug fix

    // Reverse the tags to process from right to left
    Iterator<TagWithType> reversedTags = tags.reversed().iterator();

    List<BiConsumer<ByteBuffer, Object>> writers = new ArrayList<>(tags.size());

    // Start with the leaf (rightmost) writer
    TagWithType rightmostTag = reversedTags.next();
    LOGGER.finer(() -> "*** DEBUG: Creating leaf writer for rightmost tag: " + rightmostTag.tag() + 
        " with type: " + rightmostTag.type().getSimpleName()); // TODO revert to FINER logging after bug fix
    BiConsumer<ByteBuffer, Object> writer = createLeafWriter(rightmostTag);
    writers.add(writer);

    // For nested collection or option types we Build chain from right to left (reverse order)
    while (reversedTags.hasNext()) {
      final BiConsumer<ByteBuffer, Object> lastWriter = writer; // final required for lambda capture
      TagWithType tag = reversedTags.next();
      LOGGER.finer(() -> "*** DEBUG: Processing outer tag: " + tag.tag() + 
          " with type: " + tag.type().getSimpleName()); // TODO revert to FINER logging after bug fix
      writer = switch (tag.tag()) {
        case LIST -> createListWriter(lastWriter);
        case OPTIONAL -> createOptionalWriter(lastWriter);
        case ARRAY -> {
          // Arrays use tag-based logic, not delegation
          // Get the element tag (the previous tag in the chain)
          TagWithType elementTag = structure.tagTypes().get(structure.tagTypes().size() - 1);
          yield createArrayWriter(elementTag);
        }
        case MAP -> {
          // As we are going in reverse order we have to flip the last two writers
          final var keyDelegate = writers.getLast();
          final var valueDelegate = writers.get(writers.size() - 2);
          yield createMapWriter(keyDelegate, valueDelegate);
        }
        default -> createLeafWriter(tag);
      };
      writers.add(writer);
    }

    LOGGER.finer(() -> "*** DEBUG: Final writer chain has " + writers.size() + " writers"); // TODO revert to FINER logging after bug fix
    return writer;
  }

  @SuppressWarnings("unused")
  BiConsumer<ByteBuffer, Object> createMapWriter(BiConsumer<ByteBuffer, Object> valueDelegate, BiConsumer<ByteBuffer, Object> delegate) {
    throw new UnsupportedOperationException("Map serialization not yet implemented");
  }

  /// Create leaf writer for primitive/basic types - NO runtime type checking
  BiConsumer<ByteBuffer, Object> createLeafWriter(TagWithType leafTag) {
    LOGGER.finer(() -> "*** DEBUG: Creating leaf writer for tag: " + leafTag.tag() + 
        " with type: " + leafTag.type().getSimpleName()); // TODO revert to FINER logging after bug fix
    return switch (leafTag.tag()) {
      case BOOLEAN -> BOOLEAN_WRITER;
      case BYTE -> BYTE_WRITER;
      case SHORT -> SHORT_WRITER;
      case CHARACTER -> CHAR_WRITER;
      case INTEGER -> INTEGER_WRITER;
      case LONG -> LONG_WRITER;
      case FLOAT -> FLOAT_WRITER;
      case DOUBLE -> DOUBLE_WRITER;
      case STRING -> STRING_WRITER;
      case UUID -> UUID_WRITER;
      case ARRAY -> {
        LOGGER.finer(() -> "*** DEBUG: Creating array writer for element type: " + leafTag.type().getSimpleName()); // TODO revert to FINER logging after bug fix
        yield createArrayWriter(leafTag);
      }
      case ENUM -> createEnumWriter();
      case RECORD -> createRecordWriter();
      default -> throw new IllegalArgumentException("No leaf writer for tag: " + leafTag);
    };
  }

  // Pre-built type-specific writers - these are created once and reused
  static final BiConsumer<ByteBuffer, Object> BOOLEAN_WRITER = (buffer, value) -> {
    LOGGER.fine(() -> "Writing BOOLEAN: " + value);
    ZigZagEncoding.putInt(buffer, -1 * Constants.BOOLEAN.ordinal());
    buffer.put(((Boolean) value) ? (byte) 1 : (byte) 0);
  };

  static final BiConsumer<ByteBuffer, Object> BYTE_WRITER = (buffer, value) -> {
    LOGGER.fine(() -> "Writing BYTE: " + value);
    ZigZagEncoding.putInt(buffer, -1 * Constants.BYTE.ordinal());
    buffer.put((Byte) value);
  };

  static final BiConsumer<ByteBuffer, Object> SHORT_WRITER = (buffer, value) -> {
    LOGGER.fine(() -> "Writing SHORT: " + value);
    ZigZagEncoding.putInt(buffer, -1 * Constants.SHORT.ordinal());
    buffer.putShort((Short) value);
  };

  static final BiConsumer<ByteBuffer, Object> CHAR_WRITER = (buffer, value) -> {
    LOGGER.fine(() -> "Writing CHARACTER: " + value);
    ZigZagEncoding.putInt(buffer, -1 * Constants.CHARACTER.ordinal());
    buffer.putChar((Character) value);
  };

  static final BiConsumer<ByteBuffer, Object> INTEGER_WRITER = (buffer, value) -> {
    final Integer intValue = (Integer) value;
    LOGGER.fine(() -> "Writing INTEGER: " + intValue);
    if (ZigZagEncoding.sizeOf(intValue) < Integer.BYTES) {
      ZigZagEncoding.putInt(buffer, -1 * Constants.INTEGER_VAR.ordinal());
      ZigZagEncoding.putInt(buffer, intValue);
    } else {
      ZigZagEncoding.putInt(buffer, -1 * INTEGER.ordinal());
      buffer.putInt(intValue);
    }
  };

  static final BiConsumer<ByteBuffer, Object> LONG_WRITER = (buffer, value) -> {
    final Long longValue = (Long) value;
    LOGGER.fine(() -> "Writing LONG: " + longValue);
    if (ZigZagEncoding.sizeOf(longValue) < Long.BYTES) {
      ZigZagEncoding.putInt(buffer, -1 * Constants.LONG_VAR.ordinal());
      ZigZagEncoding.putLong(buffer, longValue);
    } else {
      ZigZagEncoding.putInt(buffer, -1 * LONG.ordinal());
      buffer.putLong(longValue);
    }
  };

  static final BiConsumer<ByteBuffer, Object> FLOAT_WRITER = (buffer, value) -> {
    LOGGER.fine(() -> "Writing FLOAT: " + value);
    ZigZagEncoding.putInt(buffer, -1 * Constants.FLOAT.ordinal());
    buffer.putFloat((Float) value);
  };

  static final BiConsumer<ByteBuffer, Object> DOUBLE_WRITER = (buffer, value) -> {
    LOGGER.fine(() -> "Writing DOUBLE: " + value);
    ZigZagEncoding.putInt(buffer, -1 * Constants.DOUBLE.ordinal());
    buffer.putDouble((Double) value);
  };

  static final BiConsumer<ByteBuffer, Object> STRING_WRITER = (buffer, value) -> {
    LOGGER.fine(() -> "Writing STRING: " + value);
    ZigZagEncoding.putInt(buffer, -1 * Constants.STRING.ordinal());
    byte[] bytes = ((String) value).getBytes(UTF_8);
    ZigZagEncoding.putInt(buffer, bytes.length);
    buffer.put(bytes);
  };

  static final BiConsumer<ByteBuffer, Object> UUID_WRITER = (buffer, value) -> {
    LOGGER.fine(() -> "Writing UUID: " + value);
    ZigZagEncoding.putInt(buffer, -1 * Constants.UUID.ordinal());
    UUID uuid = (UUID) value;
    buffer.putLong(uuid.getMostSignificantBits());
    buffer.putLong(uuid.getLeastSignificantBits());
  };

  /// Create array writer that delegates to element writer
  BiConsumer<ByteBuffer, Object> createArrayWriter(final TagWithType typeWithTag) {
    final var outerMarker = -1 * Constants.ARRAY.ordinal();
    LOGGER.finer(() -> "*** DEBUG: Creating array writer for element tag: " + typeWithTag.tag() + 
        " element type: " + typeWithTag.type().getSimpleName() + 
        " outerMarker: " + outerMarker); // TODO revert to FINER logging after bug fix
    return switch (typeWithTag.tag()) {
      case BYTE -> (buffer, value) -> {
        final var bytes = (byte[]) value;
        LOGGER.finer(() -> "*** DEBUG: Writing BYTE array - position=" + buffer.position() + " value=" + Arrays.toString(bytes)); // TODO revert to FINER logging after bug fix
        LOGGER.finer(() -> "*** DEBUG: Writing ARRAY outer marker: " + outerMarker); // TODO revert to FINER logging after bug fix
        ZigZagEncoding.putInt(buffer, outerMarker);
        LOGGER.finer(() -> "*** DEBUG: Writing BYTE element marker: " + (-1 * Constants.BYTE.ordinal())); // TODO revert to FINER logging after bug fix
        ZigZagEncoding.putInt(buffer, -1 * Constants.BYTE.ordinal());
        int length = bytes.length;
        LOGGER.finer(() -> "*** DEBUG: Writing BYTE array length=" + length); // TODO revert to FINER logging after bug fix
        ZigZagEncoding.putInt(buffer, length);
        LOGGER.finer(() -> "*** DEBUG: Writing BYTE array data: " + Arrays.toString(bytes)); // TODO revert to FINER logging after bug fix
        buffer.put(bytes);
        LOGGER.finer(() -> "*** DEBUG: Finished writing BYTE array, final position: " + buffer.position()); // TODO revert to FINER logging after bug fix
      };
      case BOOLEAN -> (buffer, value) -> {
        final var booleans = (boolean[]) value;
        LOGGER.fine(() -> "Writing BOOLEAN array - position=" + buffer.position() + " value=" + Arrays.toString(booleans));
        ZigZagEncoding.putInt(buffer, outerMarker);
        ZigZagEncoding.putInt(buffer, -1 * Constants.BOOLEAN.ordinal());
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
          ZigZagEncoding.putInt(buffer, length);
          for (int i : integers) {
            ZigZagEncoding.putInt(buffer, i);
          }
        } else {
          LOGGER.fine(() -> "Writing INTEGER array - position=" + buffer.position() + " length=" + length);
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
          ZigZagEncoding.putInt(buffer, length);
          for (long i : longs) {
            ZigZagEncoding.putLong(buffer, i);
          }
        } else {
          LOGGER.fine(() -> "Writing LONG array - position=" + buffer.position() + " length=" + length);
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
        ZigZagEncoding.putInt(buffer, -1 * Constants.FLOAT.ordinal());
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
        ZigZagEncoding.putInt(buffer, -1 * Constants.DOUBLE.ordinal());
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
        ZigZagEncoding.putInt(buffer, -1 * Constants.STRING.ordinal());
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
        ZigZagEncoding.putInt(buffer, -1 * Constants.UUID.ordinal());
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
        ZigZagEncoding.putInt(buffer, -1 * Constants.SHORT.ordinal());
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
        ZigZagEncoding.putInt(buffer, -1 * Constants.CHARACTER.ordinal());
        ZigZagEncoding.putInt(buffer, length);
        for (char c : chars) {
          buffer.putChar(c);
        }
      };
      case OPTIONAL -> (buffer, value) -> {
        final var optional = (Optional<?>) value;
        LOGGER.fine(() -> "Writing OPTIONAL array - position=" + buffer.position() + " value=" +optional);
        ZigZagEncoding.putInt(buffer, outerMarker);
          if (optional.isEmpty()) {
            ZigZagEncoding.putInt(buffer, -1 * Constants.OPTIONAL_EMPTY.ordinal());
          } else {
            ZigZagEncoding.putInt(buffer, -1 * Constants.OPTIONAL_OF.ordinal());
            Object innerValue = optional.get();
            createLeafWriter(typeWithTag).accept(buffer, innerValue);
          }
      };
      default ->
          throw new IllegalArgumentException("Unsupported array type for direct writer: " + typeWithTag.tag());
    };
  }

  /// Create enum writer that has access to instance fields for class ordinal lookup
  BiConsumer<ByteBuffer, Object> createEnumWriter() {
    return (buffer, value) -> {
      Enum<?> enumValue = (Enum<?>) value;
      ZigZagEncoding.putInt(buffer, -1 * Constants.ENUM.ordinal());
      Integer ordinal = classToOrdinal.get(enumValue.getClass());
      ZigZagEncoding.putInt(buffer, ordinal + 1);
      String constantName = enumValue.name();
      byte[] constantBytes = constantName.getBytes(UTF_8);
      ZigZagEncoding.putInt(buffer, constantBytes.length);
      buffer.put(constantBytes);
    };
  }

  /// Create record writer that has access to instance fields
  BiConsumer<ByteBuffer, Object> createRecordWriter() {
    return (buffer, value) -> {
      LOGGER.fine(() -> "Writing nested RECORD: " + value.getClass().getSimpleName());
      ZigZagEncoding.putInt(buffer, -1 * Constants.RECORD.ordinal());
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
        ZigZagEncoding.putInt(buffer, -1 * Constants.OPTIONAL_EMPTY.ordinal());
      } else {
        LOGGER.fine(() -> "Writing OPTIONAL_OF");
        ZigZagEncoding.putInt(buffer, -1 * Constants.OPTIONAL_OF.ordinal());
        elementWriter.accept(buffer, optional.get());
      }
    };
  }

  /// Create writer for List values
  BiConsumer<ByteBuffer, Object> createListWriter(BiConsumer<ByteBuffer, Object> elementWriter) {
    return (buffer, value) -> {
      List<?> list = (List<?>) value;
      LOGGER.fine(() -> "Writing LIST size=" + list.size());
      ZigZagEncoding.putInt(buffer, -1 * Constants.LIST.ordinal());
      buffer.putInt(list.size());
      for (Object item : list) {
        elementWriter.accept(buffer, item);
      }
    };
  }

  /// Build reader chain for a component
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
    if (tags.isEmpty()) { // TODO pull this up to the call site
      throw new IllegalArgumentException("Type structure must have at least one tag");
    }

    // For complex types, build chain from right to left
    Iterator<TagWithType> reversedTags = tags.reversed().iterator();
    TagWithType rightmostTag = reversedTags.next();
    Function<ByteBuffer, Object> reader = createLeafReader(rightmostTag);

    while (reversedTags.hasNext()) {
      final Function<ByteBuffer, Object> innerReader = reader;
      TagWithType tag = reversedTags.next();
      reader = switch (tag.tag()) {
        case LIST -> createListReader(innerReader);
        case OPTIONAL -> createOptionalReader(innerReader);
        case ARRAY -> createArrayReader();
        default -> createLeafReader(tag);
      };
    }

    return reader;
  }

  /// Create leaf reader for primitive/basic types
  Function<ByteBuffer, Object> createLeafReader(TagWithType leafTag) {
    return switch (leafTag.tag()) {
      case BOOLEAN -> BOOLEAN_READER;
      case BYTE -> BYTE_READER;
      case SHORT -> SHORT_READER;
      case CHARACTER -> CHAR_READER;
      case INTEGER -> INTEGER_READER;
      case LONG -> LONG_READER;
      case FLOAT -> FLOAT_READER;
      case DOUBLE -> DOUBLE_READER;
      case STRING -> STRING_READER;
      case UUID -> UUID_READER;
      case ARRAY -> createArrayReader();
      case ENUM -> createEnumReader();
      case RECORD -> createRecordReader();
      default -> throw new IllegalArgumentException("No leaf reader for tag: " + leafTag);
    };
  }

  // Pre-built type-specific readers
  static final Function<ByteBuffer, Object> BOOLEAN_READER = buffer -> {
    int marker = ZigZagEncoding.getInt(buffer);
    if (marker != -1 * Constants.BOOLEAN.ordinal()) {
      throw new IllegalStateException("Expected BOOLEAN marker but got: " + marker);
    }
    boolean value = buffer.get() != 0;
    LOGGER.fine(() -> "Read Boolean: " + value);
    return value;
  };

  static final Function<ByteBuffer, Object> BYTE_READER = buffer -> {
    int marker = ZigZagEncoding.getInt(buffer);
    if (marker != -1 * Constants.BYTE.ordinal()) {
      throw new IllegalStateException("Expected BYTE marker but got: " + marker);
    }
    byte value = buffer.get();
    LOGGER.fine(() -> "Read Byte: " + value);
    return value;
  };

  static final Function<ByteBuffer, Object> SHORT_READER = buffer -> {
    int marker = ZigZagEncoding.getInt(buffer);
    if (marker != -1 * Constants.SHORT.ordinal()) {
      throw new IllegalStateException("Expected SHORT marker but got: " + marker);
    }
    short value = buffer.getShort();
    LOGGER.fine(() -> "Read Short: " + value);
    return value;
  };

  static final Function<ByteBuffer, Object> CHAR_READER = buffer -> {
    int marker = ZigZagEncoding.getInt(buffer);
    if (marker != -1 * Constants.CHARACTER.ordinal()) {
      throw new IllegalStateException("Expected CHARACTER marker but got: " + marker);
    }
    char value = buffer.getChar();
    LOGGER.fine(() -> "Read Character: " + value);
    return value;
  };

  static final Function<ByteBuffer, Object> INTEGER_READER = buffer -> {
    int marker = ZigZagEncoding.getInt(buffer);
    if (marker == -1 * Constants.INTEGER_VAR.ordinal()) {
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

  static final Function<ByteBuffer, Object> LONG_READER = buffer -> {
    int marker = ZigZagEncoding.getInt(buffer);
    if (marker == -1 * Constants.LONG_VAR.ordinal()) {
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

  static final Function<ByteBuffer, Object> FLOAT_READER = buffer -> {
    int marker = ZigZagEncoding.getInt(buffer);
    if (marker != -1 * Constants.FLOAT.ordinal()) {
      throw new IllegalStateException("Expected FLOAT marker but got: " + marker);
    }
    float value = buffer.getFloat();
    LOGGER.fine(() -> "Read Float: " + value);
    return value;
  };

  static final Function<ByteBuffer, Object> DOUBLE_READER = buffer -> {
    int marker = ZigZagEncoding.getInt(buffer);
    if (marker != -1 * Constants.DOUBLE.ordinal()) {
      throw new IllegalStateException("Expected DOUBLE marker but got: " + marker);
    }
    double value = buffer.getDouble();
    LOGGER.fine(() -> "Read Double: " + value);
    return value;
  };

  static final Function<ByteBuffer, Object> STRING_READER = buffer -> {
    int marker = ZigZagEncoding.getInt(buffer);
    if (marker != -1 * Constants.STRING.ordinal()) {
      throw new IllegalStateException("Expected STRING marker but got: " + marker);
    }
    int length = ZigZagEncoding.getInt(buffer);
    byte[] bytes = new byte[length];
    buffer.get(bytes);
    String value = new String(bytes, UTF_8);
    LOGGER.fine(() -> "Read String: " + value);
    return value;
  };

  static final Function<ByteBuffer, Object> UUID_READER = buffer -> {
    int marker = ZigZagEncoding.getInt(buffer);
    if (marker != -1 * Constants.UUID.ordinal()) {
      throw new IllegalStateException("Expected UUID marker but got: " + marker);
    }
    final long mostSigBits = buffer.getLong();
    final long leastSigBits = buffer.getLong();
    UUID value = new UUID(mostSigBits, leastSigBits);
    LOGGER.fine(() -> "Read UUID: " + value);
    return value;
  };

  /// Create array reader that has access to instance fields for record arrays
  Function<ByteBuffer, Object> createArrayReader() {
    return buffer -> {
      LOGGER.finer(() -> "*** DEBUG: Reading ARRAY at position " + buffer.position()); // TODO revert to FINER logging after bug fix
      int marker = ZigZagEncoding.getInt(buffer);
      LOGGER.finer(() -> "*** DEBUG: Read ARRAY marker: " + marker + " expected: " + (-1 * Constants.ARRAY.ordinal())); // TODO revert to FINER logging after bug fix
      if (marker != -1 * Constants.ARRAY.ordinal()) {
        throw new IllegalStateException("Expected ARRAY marker but got: " + marker);
      }
      int arrayTypeMarker = ZigZagEncoding.getInt(buffer);
      LOGGER.finer(() -> "*** DEBUG: Read array type marker: " + arrayTypeMarker + " constant: " + CONSTANTS_ARRAY[-1 * arrayTypeMarker]); // TODO revert to FINER logging after bug fix
      return switch (CONSTANTS_ARRAY[-1 * arrayTypeMarker]) {
        case BOOLEAN -> {
          int boolLength = ZigZagEncoding.getInt(buffer);
          boolean[] booleans = new boolean[boolLength];
          int bytesLength = ZigZagEncoding.getInt(buffer);
          byte[] bytes = new byte[bytesLength];
          buffer.get(bytes);
          BitSet bitSet = BitSet.valueOf(bytes);
          IntStream.range(0, boolLength).forEach(i -> booleans[i] = bitSet.get(i));
          yield booleans;
        }
        case BYTE -> {
          int length = ZigZagEncoding.getInt(buffer);
          byte[] bytes = new byte[length];
          buffer.get(bytes);
          yield bytes;
        }
        case SHORT -> {
          int length = ZigZagEncoding.getInt(buffer);
          short[] shorts = new short[length];
          IntStream.range(0, length).forEach(i -> shorts[i] = buffer.getShort());
          yield shorts;
        }
        case CHARACTER -> {
          int length = ZigZagEncoding.getInt(buffer);
          char[] chars = new char[length];
          IntStream.range(0, length).forEach(i -> chars[i] = buffer.getChar());
          yield chars;
        }
        case INTEGER -> {
          int length = ZigZagEncoding.getInt(buffer);
          int[] integers = new int[length];
          Arrays.setAll(integers, i -> buffer.getInt());
          yield integers;
        }
        case INTEGER_VAR -> {
          int length = ZigZagEncoding.getInt(buffer);
          int[] integers = new int[length];
          Arrays.setAll(integers, i -> ZigZagEncoding.getInt(buffer));
          yield integers;
        }
        case LONG -> {
          int length = ZigZagEncoding.getInt(buffer);
          long[] longs = new long[length];
          Arrays.setAll(longs, i -> buffer.getLong());
          yield longs;
        }
        case LONG_VAR -> {
          int length = ZigZagEncoding.getInt(buffer);
          long[] longs = new long[length];
          Arrays.setAll(longs, i -> ZigZagEncoding.getLong(buffer));
          yield longs;
        }
        case FLOAT -> {
          int length = ZigZagEncoding.getInt(buffer);
          float[] floats = new float[length];
          IntStream.range(0, length).forEach(i -> floats[i] = buffer.getFloat());
          yield floats;
        }
        case DOUBLE -> {
          int length = ZigZagEncoding.getInt(buffer);
          double[] doubles = new double[length];
          IntStream.range(0, length).forEach(i -> doubles[i] = buffer.getDouble());
          yield doubles;
        }
        case STRING -> {
          int length = ZigZagEncoding.getInt(buffer);
          String[] strings = new String[length];
          IntStream.range(0, length).forEach(i -> {
            int strLength = ZigZagEncoding.getInt(buffer);
            byte[] bytes = new byte[strLength];
            buffer.get(bytes);
            strings[i] = new String(bytes, UTF_8);
          });
          yield strings;
        }
        case UUID -> {
          int length = ZigZagEncoding.getInt(buffer);
          UUID[] uuids = new UUID[length];
          IntStream.range(0, length).forEach(i -> {
            long mostSigBits = buffer.getLong();
            long leastSigBits = buffer.getLong();
            uuids[i] = new UUID(mostSigBits, leastSigBits);
          });
          yield uuids;
        }
        case RECORD -> {
          int length = ZigZagEncoding.getInt(buffer);
          int componentWireOrdinal = ZigZagEncoding.getInt(buffer);
          int componentOrdinal = componentWireOrdinal - 1;
          Class<?> componentType = userTypes[componentOrdinal];
          Record[] records = (Record[]) Array.newInstance(componentType, length);
          Arrays.setAll(records, i -> deserializeRecord(buffer, componentOrdinal));
          yield records;
        }
        default -> throw new IllegalStateException("Unsupported array type marker: " + arrayTypeMarker);
      };
    };
  }

  /// Create enum reader that has access to instance fields for ordinal lookup
  Function<ByteBuffer, Object> createEnumReader() {
    return buffer -> {
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
  }

  /// Create record reader that has access to instance fields
  Function<ByteBuffer, Object> createRecordReader() {
    return buffer -> {
      int marker = ZigZagEncoding.getInt(buffer);
      if (marker != -1 * Constants.RECORD.ordinal()) {
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
  }

  /// Create reader for Optional values
  Function<ByteBuffer, Object> createOptionalReader(Function<ByteBuffer, Object> elementReader) {
    return buffer -> {
      int marker = ZigZagEncoding.getInt(buffer);
      if (marker == -1 * Constants.OPTIONAL_EMPTY.ordinal()) {
        LOGGER.fine(() -> "Reading OPTIONAL_EMPTY");
        return Optional.empty();
      } else if (marker == -1 * Constants.OPTIONAL_OF.ordinal()) {
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
      if (marker != -1 * Constants.LIST.ordinal()) {
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
        case ARRAY -> ARRAY_SIZER;
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
      case INTEGER -> INTEGER_SIZER;
      case LONG -> LONG_SIZER;
      case FLOAT -> obj -> 1 + Float.BYTES;
      case DOUBLE -> obj -> 1 + Double.BYTES;
      case STRING -> STRING_SIZER;
      case UUID -> obj -> 1 + 2 * Long.BYTES;
      case ARRAY -> ARRAY_SIZER;
      case ENUM -> createEnumSizer();
      case RECORD -> createRecordSizer();
      default -> throw new IllegalArgumentException("No leaf sizer for tag: " + leafTag);
    };
  }

  // Pre-built type-specific sizers
  static final ToIntFunction<Object> INTEGER_SIZER = obj -> {
    Integer value = (Integer) obj;
    int zigzagSize = ZigZagEncoding.sizeOf(value);
    return 1 + (Math.min(zigzagSize, Integer.BYTES));
  };

  static final ToIntFunction<Object> LONG_SIZER = obj -> {
    Long value = (Long) obj;
    int zigzagSize = ZigZagEncoding.sizeOf(value);
    return 1 + (Math.min(zigzagSize, Long.BYTES));
  };

  static final ToIntFunction<Object> STRING_SIZER = obj -> {
    String s = (String) obj;
    byte[] bytes = s.getBytes(UTF_8);
    return 1 + ZigZagEncoding.sizeOf(bytes.length) + bytes.length;
  };

  // TODO this probably does not cover all array types, but is a good start
  static final ToIntFunction<Object> ARRAY_SIZER = value -> {
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
        int contentSize = Arrays.stream(strings).mapToInt(s -> STRING_SIZER.applyAsInt(s) - 1).sum();
        yield overhead + contentSize;
      }
      case UUID[] uuids -> 1 + 4 + uuids.length * (2 * Long.BYTES);
      default -> throw new AssertionError("not implemented for array type: " + value.getClass());
    };
  };

  /// Create enum sizer that has access to instance fields for class ordinal lookup
  ToIntFunction<Object> createEnumSizer() {
    return obj -> {
      Enum<?> enumValue = (Enum<?>) obj;
      Integer ordinal = classToOrdinal.get(enumValue.getClass());
      String constantName = enumValue.name();
      byte[] nameBytes = constantName.getBytes(UTF_8);
      return 1 + ZigZagEncoding.sizeOf(ordinal + 1) + ZigZagEncoding.sizeOf(nameBytes.length) + nameBytes.length;
    };
  }

  /// Create record sizer that has access to instance fields
  ToIntFunction<Object> createRecordSizer() {
    return obj -> {
      Record record = (Record) obj;
      Integer ordinal = classToOrdinal.get(record.getClass());
      if (ordinal != null) {
        return ZigZagEncoding.sizeOf(ordinal + 1) + maxSizeOfRecordComponents(record, ordinal);
      } else {
        throw new IllegalArgumentException("Unknown record type: " + record.getClass());
      }
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
      for (Object item : list) {
        size += elementSizer.applyAsInt(item);
      }
      return size;
    };
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
      } else if (current instanceof Class<?> clazz) {
        if (clazz.isArray()) {
          // Array container with element type, e.g. short[] -> [ARRAY, SHORT]
          tags.add(ARRAY);
          types.add(Arrays.class); // Arrays.class as marker, not concrete array type
          Class<?> componentType = clazz.getComponentType();
          tags.add(fromClass(componentType)); // Element tag
          types.add(componentType); // Element type
        } else {
          // Regular class - terminal case
          tags.add(fromClass(clazz));
          types.add(clazz);
        }
        return new TypeStructure(tags, types);
      } else {
        // Unknown type, return what we have
        return new TypeStructure(tags, types);
      }
    }

    return new TypeStructure(tags, types);
  }
}
