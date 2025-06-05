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
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static io.github.simbo1905.no.framework.Pickler.LOGGER;

/// Unified pickler implementation that handles all reachable types using array-based architecture.
/// Eliminates the need for separate RecordPickler and SealedPickler classes.
final class PicklerImpl<T> implements Pickler<T> {

  // Static final array for O(1) Constants lookup without defensive copying on hot path
  static final Constants[] CONSTANTS_ARRAY = Constants.values();
  static final int SAMPLE_SIZE = 32;

  // Global lookup tables indexed by ordinal - the core of the unified architecture
  final Class<?>[] discoveredClasses;     // Lexicographically sorted user types
  final Map<Class<?>, Integer> classToOrdinal;  // Fast class to ordinal lookup (the ONE HashMap)
  
  // Pre-built component metadata arrays for all discovered record types - the core performance optimization
  final MethodHandle[] recordConstructors;      // Constructor method handles indexed by ordinal
  final MethodHandle[][] componentAccessors;    // [recordOrdinal][componentIndex] -> accessor method handle
  final TypeStructure[][] componentTypes;       // [recordOrdinal][componentIndex] -> type structure
  final BiConsumer<ByteBuffer, Object>[][] componentWriters;  // [recordOrdinal][componentIndex] -> writer lambda
  final Function<ByteBuffer, Object>[][] componentReaders;    // [recordOrdinal][componentIndex] -> reader lambda
  final java.util.function.ToIntFunction<Object>[][] componentSizers; // [recordOrdinal][componentIndex] -> sizer lambda

  /// Create a unified pickler for any root type (record, enum, or sealed interface)
  PicklerImpl(Class<T> rootClass) {
    Objects.requireNonNull(rootClass, "rootClass cannot be null");

    LOGGER.info(() -> "Creating unified pickler for root class: " + rootClass.getName());

    // Phase 1: Discover all reachable user types using recordClassHierarchy
    Set<Class<?>> allReachableClasses = recordClassHierarchy(rootClass, new HashSet<>())
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

    // Build the ONE HashMap for class->ordinal lookup (O(1) for hot path)
    this.classToOrdinal = IntStream.range(0, discoveredClasses.length)
        .boxed()
        .collect(Collectors.toMap(i -> discoveredClasses[i], i -> i));
    
    LOGGER.info(() -> "Built classToOrdinal map: " + classToOrdinal.entrySet().stream()
        .map(e -> e.getKey().getSimpleName() + "->" + e.getValue()).toList());
    
    // Pre-allocate metadata arrays for all discovered record types (array-based for O(1) access)
    int numRecordTypes = discoveredClasses.length;
    this.recordConstructors = new MethodHandle[numRecordTypes];
    this.componentAccessors = new MethodHandle[numRecordTypes][];
    this.componentTypes = new TypeStructure[numRecordTypes][];
    this.componentWriters = new BiConsumer[numRecordTypes][];
    this.componentReaders = new Function[numRecordTypes][];
    this.componentSizers = new java.util.function.ToIntFunction[numRecordTypes][];
    
    // Build metadata for each discovered record type using meta-programming
    for (int ordinal = 0; ordinal < numRecordTypes; ordinal++) {
      Class<?> recordClass = discoveredClasses[ordinal];
      if (recordClass.isRecord()) {
        buildRecordMetadata(ordinal, recordClass);
      }
    }
    
    LOGGER.info(() -> "PicklerImpl construction complete - ready for high-performance serialization");
  }
  
  /// Build component metadata for a record type using meta-programming (like RecordReflection.analyze)
  @SuppressWarnings("unchecked")
  void buildRecordMetadata(int ordinal, Class<?> recordClass) {
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
    componentSizers[ordinal] = new java.util.function.ToIntFunction[numComponents];
    
    for (int i = 0; i < numComponents; i++) {
      RecordComponent component = components[i];
      
      // Get accessor method handle
      try {
        componentAccessors[ordinal][i] = MethodHandles.lookup().unreflect(component.getAccessor());
      } catch (IllegalAccessException e) {
        throw new RuntimeException("Failed to unreflect accessor for " + component.getName(), e);
      }
      
      // Analyze component type structure
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
    
    LOGGER.finer(() -> "Serializing " + writers.length + " components for " + record.getClass().getSimpleName());
    
    // Use pre-built writers - direct array access, no HashMap lookups
    for (BiConsumer<ByteBuffer, Object> writer : writers) {
      writer.accept(buffer, record);
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
    
    LOGGER.finer(() -> "Deserializing " + readers.length + " components");
    
    // Read components using pre-built readers
    Object[] components = new Object[readers.length];
    for (int i = 0; i < readers.length; i++) {
      components[i] = readers[i].apply(buffer);
    }
    
    // Invoke constructor
    try {
      return (T) constructor.invokeWithArguments(components);
    } catch (Throwable e) {
      throw new RuntimeException("Failed to construct record", e);
    }
  }
  
  /// Calculate max size using pre-built component sizers
  int maxSizeOfRecordComponents(Record record, int ordinal) {
    java.util.function.ToIntFunction<Object>[] sizers = componentSizers[ordinal];
    if (sizers == null) {
      throw new IllegalStateException("No sizers for ordinal: " + ordinal);
    }
    
    int totalSize = 0;
    for (java.util.function.ToIntFunction<Object> sizer : sizers) {
      totalSize += sizer.applyAsInt(record);
    }
    
    return totalSize;
  }
  
  /// Build writer chain for a component - creates type-specific writers at construction time
  BiConsumer<ByteBuffer, Object> buildWriterChain(TypeStructure typeStructure, MethodHandle accessor) {
    // Build the type-specific writer chain
    BiConsumer<ByteBuffer, Object> typeWriter = buildTypeWriterChain(typeStructure);
    
    // Extract component value with null guard then delegate to type-specific writer
    return (buffer, record) -> {
      try {
        Object componentValue = accessor.invokeWithArguments(record);
        if (componentValue == null) {
          ZigZagEncoding.putInt(buffer, -1 * Constants.NULL.ordinal());
          LOGGER.fine(() -> "Writing NULL component");
        } else {
          typeWriter.accept(buffer, componentValue);
        }
      } catch (Throwable e) {
        throw new RuntimeException("Failed to write component", e);
      }
    };
  }
  
  /// Build type-specific writer chain based on TypeStructure (like Machinary's buildWriterChain)
  BiConsumer<ByteBuffer, Object> buildTypeWriterChain(TypeStructure structure) {
    List<Tag> tags = structure.tags();
    List<Class<?>> types = structure.types();
    if (tags.isEmpty()) {
      throw new IllegalArgumentException("Type structure must have at least one tag");
    }
    LOGGER.fine(() -> "Building writer chain for tags: " + tags);
    
    // For simple types (single tag), return leaf writer directly
    if (tags.size() == 1) {
      return createLeafWriter(tags.get(0));
    }
    
    // For complex types, build chain from right to left (like backup code)
    Iterator<Tag> reversedTags = tags.reversed().iterator();
    
    // Start with the leaf (rightmost) writer
    Tag rightmostTag = reversedTags.next();
    BiConsumer<ByteBuffer, Object> writer = createLeafWriter(rightmostTag);
    
    // Build chain from right to left (reverse order)
    while (reversedTags.hasNext()) {
      final BiConsumer<ByteBuffer, Object> innerWriter = writer;
      Tag tag = reversedTags.next();
      
      writer = switch (tag) {
        case LIST -> createListWriter(innerWriter);
        case OPTIONAL -> createOptionalWriter(innerWriter);
        case ARRAY -> createArrayWriter(innerWriter, rightmostTag);
        default -> createLeafWriter(tag);
      };
      
      // Update rightmostTag for next iteration
      rightmostTag = tag;
    }
    
    return writer;
  }
  
  /// Create leaf writer for primitive/basic types - NO runtime type checking
  BiConsumer<ByteBuffer, Object> createLeafWriter(Tag leafTag) {
    LOGGER.fine(() -> "Creating leaf writer for tag: " + leafTag);
    return switch (leafTag) {
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
      case ARRAY -> throw new IllegalArgumentException("ARRAY should not be a leaf tag - needs delegation");
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
      ZigZagEncoding.putInt(buffer, -1 * Constants.INTEGER.ordinal());
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
      ZigZagEncoding.putInt(buffer, -1 * Constants.LONG.ordinal());
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
    byte[] bytes = ((String) value).getBytes(StandardCharsets.UTF_8);
    ZigZagEncoding.putInt(buffer, bytes.length);
    buffer.put(bytes);
  };
  
  static final BiConsumer<ByteBuffer, Object> UUID_WRITER = (buffer, value) -> {
    LOGGER.fine(() -> "Writing UUID: " + value);
    ZigZagEncoding.putInt(buffer, -1 * Constants.UUID.ordinal());
    java.util.UUID uuid = (java.util.UUID) value;
    buffer.putLong(uuid.getMostSignificantBits());
    buffer.putLong(uuid.getLeastSignificantBits());
  };
  
  /// Create array writer that delegates to element writer
  BiConsumer<ByteBuffer, Object> createArrayWriter(BiConsumer<ByteBuffer, Object> elementWriter, Tag elementTag) {
    return (buffer, value) -> {
      LOGGER.fine(() -> "Writing ARRAY with element tag: " + elementTag);
      ZigZagEncoding.putInt(buffer, -1 * Constants.ARRAY.ordinal());
      
      // Write element type ordinal based on static analysis (elementTag from TypeStructure)
      switch (elementTag) {
        case INTEGER -> {
          // For int arrays, decide compression at runtime
          int[] ints = (int[]) value;
          int sampleAvg = ints.length > 0 ? estimateAverageSizeInt(ints, ints.length) : 1;
          Constants marker = ((ints.length <= SAMPLE_SIZE && sampleAvg < Integer.BYTES - 1) ||
                             (ints.length > SAMPLE_SIZE && sampleAvg < Integer.BYTES - 2)) 
                             ? Constants.INTEGER_VAR : Constants.INTEGER;
          ZigZagEncoding.putInt(buffer, -1 * marker.ordinal());
        }
        case LONG -> {
          // For long arrays, decide compression at runtime
          long[] longs = (long[]) value;
          int sampleAvg = longs.length > 0 ? estimateAverageSizeLong(longs, longs.length) : 1;
          Constants marker = ((longs.length <= SAMPLE_SIZE && sampleAvg < Long.BYTES - 1) ||
                             (longs.length > SAMPLE_SIZE && sampleAvg < Long.BYTES - 2))
                             ? Constants.LONG_VAR : Constants.LONG;
          ZigZagEncoding.putInt(buffer, -1 * marker.ordinal());
        }
        case RECORD, ENUM -> {
          // User type: get component type and write 1-indexed ordinal
          Class<?> componentType = value.getClass().getComponentType();
          Integer userOrdinal = classToOrdinal.get(componentType);
          if (userOrdinal == null) {
            throw new IllegalArgumentException("Unknown user type: " + componentType);
          }
          ZigZagEncoding.putInt(buffer, userOrdinal + 1);
        }
        default -> {
          // Other built-in types: find Constants with matching tag and write negative ordinal
          Constants constant = Arrays.stream(Constants.values())
              .filter(c -> elementTag.equals(c.tag))
              .findFirst()
              .orElseThrow(() -> new IllegalArgumentException("No Constants mapping for tag: " + elementTag));
          ZigZagEncoding.putInt(buffer, -1 * constant.ordinal());
        }
      }
      
      // Write array length
      int length = Array.getLength(value);
      ZigZagEncoding.putInt(buffer, length);
      
      // Write elements based on actual array type (no additional markers - already written above)
      switch (value) {
        case byte[] arr -> {
          buffer.put(arr);
        }
        case boolean[] booleans -> {
          BitSet bitSet = new BitSet(booleans.length);
          IntStream.range(0, booleans.length)
              .filter(i -> booleans[i])
              .forEach(bitSet::set);
          byte[] bytes = bitSet.toByteArray();
          ZigZagEncoding.putInt(buffer, bytes.length);
          buffer.put(bytes);
        }
        case int[] integers -> {
          int sampleAverageSize = integers.length > 0 ? estimateAverageSizeInt(integers, integers.length) : 1;
          if ((integers.length <= SAMPLE_SIZE && sampleAverageSize < Integer.BYTES - 1) ||
              (integers.length > SAMPLE_SIZE && sampleAverageSize < Integer.BYTES - 2)) {
            // Use ZigZag compression for small values
            for (int i : integers) {
              ZigZagEncoding.putInt(buffer, i);
            }
          } else {
            // Use fixed-size encoding for large values
            for (int i : integers) {
              buffer.putInt(i);
            }
          }
        }
        case long[] longs -> {
          int sampleAverageSize = longs.length > 0 ? estimateAverageSizeLong(longs, longs.length) : 1;
          if ((longs.length <= SAMPLE_SIZE && sampleAverageSize < Long.BYTES - 1) ||
              (longs.length > SAMPLE_SIZE && sampleAverageSize < Long.BYTES - 2)) {
            // Use ZigZag compression for small values
            for (long l : longs) {
              ZigZagEncoding.putLong(buffer, l);
            }
          } else {
            // Use fixed-size encoding for large values
            for (long l : longs) {
              buffer.putLong(l);
            }
          }
        }
        case float[] floats -> {
          for (float f : floats) buffer.putFloat(f);
        }
        case double[] doubles -> {
          for (double d : doubles) buffer.putDouble(d);
        }
        case short[] shorts -> {
          for (short s : shorts) buffer.putShort(s);
        }
        case char[] chars -> {
          for (char c : chars) buffer.putChar(c);
        }
        case String[] strings -> {
          ZigZagEncoding.putInt(buffer, -1 * Constants.STRING.ordinal());
          ZigZagEncoding.putInt(buffer, strings.length);
          for (String s : strings) {
            byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
            ZigZagEncoding.putInt(buffer, bytes.length);
            buffer.put(bytes);
          }
        }
        case UUID[] uuids -> {
          ZigZagEncoding.putInt(buffer, -1 * Constants.UUID.ordinal());
          ZigZagEncoding.putInt(buffer, uuids.length);
          for (UUID uuid : uuids) {
            buffer.putLong(uuid.getMostSignificantBits());
            buffer.putLong(uuid.getLeastSignificantBits());
          }
        }
        case Record[] records -> {
          ZigZagEncoding.putInt(buffer, -1 * Constants.RECORD.ordinal());
          ZigZagEncoding.putInt(buffer, records.length);
          if (records.length > 0) {
            // Write component type ordinal for first element
            Integer componentOrdinal = classToOrdinal.get(records[0].getClass());
            ZigZagEncoding.putInt(buffer, componentOrdinal + 1);
            // Serialize each record directly using existing method
            for (Record record : records) {
              serializeRecordComponents(buffer, record, componentOrdinal);
            }
          }
        }
        default -> throw new IllegalArgumentException("Unsupported array type: " + value.getClass());
      }
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
      byte[] constantBytes = constantName.getBytes(StandardCharsets.UTF_8);
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
      if (marker == -1 * Constants.NULL.ordinal()) {
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
    List<Tag> tags = structure.tags();
    if (tags.isEmpty()) {
      throw new IllegalArgumentException("Type structure must have at least one tag");
    }
    
    // For simple types, return leaf reader
    if (tags.size() == 1) {
      return createLeafReader(tags.get(0));
    }
    
    // For complex types, build chain from right to left
    Iterator<Tag> reversedTags = tags.reversed().iterator();
    Tag rightmostTag = reversedTags.next();
    Function<ByteBuffer, Object> reader = createLeafReader(rightmostTag);
    
    while (reversedTags.hasNext()) {
      final Function<ByteBuffer, Object> innerReader = reader;
      Tag tag = reversedTags.next();
      reader = switch (tag) {
        case LIST -> createListReader(innerReader);
        case OPTIONAL -> createOptionalReader(innerReader);
        case ARRAY -> createArrayReader();
        default -> createLeafReader(tag);
      };
    }
    
    return reader;
  }
  
  /// Create leaf reader for primitive/basic types
  Function<ByteBuffer, Object> createLeafReader(Tag leafTag) {
    return switch (leafTag) {
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
    } else if (marker == -1 * Constants.INTEGER.ordinal()) {
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
    } else if (marker == -1 * Constants.LONG.ordinal()) {
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
    String value = new String(bytes, StandardCharsets.UTF_8);
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
    LOGGER.fine(() -> "Reading ARRAY");
    int marker = ZigZagEncoding.getInt(buffer);
    if (marker != -1 * Constants.ARRAY.ordinal()) {
      throw new IllegalStateException("Expected ARRAY marker but got: " + marker);
    }
    int arrayTypeMarker = ZigZagEncoding.getInt(buffer);
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
          strings[i] = new String(bytes, StandardCharsets.UTF_8);
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
        Class<?> componentType = discoveredClasses[componentOrdinal];
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
      Class<?> enumClass = discoveredClasses[ordinal];
      
      int constantLength = ZigZagEncoding.getInt(buffer);
      byte[] constantBytes = new byte[constantLength];
      buffer.get(constantBytes);
      String constantName = new String(constantBytes, StandardCharsets.UTF_8);
      
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
      
      if (ordinal < 0 || ordinal >= discoveredClasses.length) {
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
  java.util.function.ToIntFunction<Object> buildSizerChain(TypeStructure typeStructure, MethodHandle accessor) {
    // Build the type-specific sizer
    java.util.function.ToIntFunction<Object> typeSizer = buildTypeSizerChain(typeStructure);
    
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
  java.util.function.ToIntFunction<Object> buildTypeSizerChain(TypeStructure structure) {
    List<Tag> tags = structure.tags();
    if (tags.isEmpty()) {
      throw new IllegalArgumentException("Type structure must have at least one tag");
    }
    
    // For simple types, return leaf sizer
    if (tags.size() == 1) {
      return createLeafSizer(tags.get(0));
    }
    
    // For complex types, build chain from right to left
    Iterator<Tag> reversedTags = tags.reversed().iterator();
    Tag rightmostTag = reversedTags.next();
    java.util.function.ToIntFunction<Object> sizer = createLeafSizer(rightmostTag);
    
    while (reversedTags.hasNext()) {
      final java.util.function.ToIntFunction<Object> innerSizer = sizer;
      Tag tag = reversedTags.next();
      sizer = switch (tag) {
        case LIST -> createListSizer(innerSizer);
        case OPTIONAL -> createOptionalSizer(innerSizer);
        case ARRAY -> ARRAY_SIZER;
        default -> createLeafSizer(tag);
      };
    }
    
    return sizer;
  }
  
  /// Create leaf sizer for primitive/basic types
  java.util.function.ToIntFunction<Object> createLeafSizer(Tag leafTag) {
    return switch (leafTag) {
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
  static final java.util.function.ToIntFunction<Object> INTEGER_SIZER = obj -> {
    Integer value = (Integer) obj;
    int zigzagSize = ZigZagEncoding.sizeOf(value);
    return 1 + (zigzagSize < Integer.BYTES ? zigzagSize : Integer.BYTES);
  };
  
  static final java.util.function.ToIntFunction<Object> LONG_SIZER = obj -> {
    Long value = (Long) obj;
    int zigzagSize = ZigZagEncoding.sizeOf(value);
    return 1 + (zigzagSize < Long.BYTES ? zigzagSize : Long.BYTES);
  };
  
  static final java.util.function.ToIntFunction<Object> STRING_SIZER = obj -> {
    String s = (String) obj;
    byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
    return 1 + ZigZagEncoding.sizeOf(bytes.length) + bytes.length;
  };
  
  static final java.util.function.ToIntFunction<Object> ARRAY_SIZER = value -> {
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
  java.util.function.ToIntFunction<Object> createEnumSizer() {
    return obj -> {
      Enum<?> enumValue = (Enum<?>) obj;
      Integer ordinal = classToOrdinal.get(enumValue.getClass());
      String constantName = enumValue.name();
      byte[] nameBytes = constantName.getBytes(StandardCharsets.UTF_8);
      return 1 + ZigZagEncoding.sizeOf(ordinal + 1) + ZigZagEncoding.sizeOf(nameBytes.length) + nameBytes.length;
    };
  }
  
  /// Create record sizer that has access to instance fields
  java.util.function.ToIntFunction<Object> createRecordSizer() {
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
  java.util.function.ToIntFunction<Object> createOptionalSizer(java.util.function.ToIntFunction<Object> elementSizer) {
    return obj -> {
      Optional<?> optional = (Optional<?>) obj;
      if (optional.isEmpty()) {
        return 1; // OPTIONAL_EMPTY marker
      } else {
        return 1 + elementSizer.applyAsInt(optional.get()); // OPTIONAL_OF marker + element
      }
    };
  }
  
  /// Create sizer for List values
  java.util.function.ToIntFunction<Object> createListSizer(java.util.function.ToIntFunction<Object> elementSizer) {
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
    Objects.requireNonNull(buffer, "buffer cannot be null");
    Objects.requireNonNull(object, "object cannot be null");
    buffer.order(java.nio.ByteOrder.BIG_ENDIAN);
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
    buffer.order(java.nio.ByteOrder.BIG_ENDIAN);
    LOGGER.finer(() -> "PicklerImpl.deserialize: starting at position=" + buffer.position());

    // Read ordinal marker (1-indexed on wire, convert to 0-indexed for array access)
    final int wireOrdinal = ZigZagEncoding.getInt(buffer);
    final int ordinal = wireOrdinal - 1;
    
    LOGGER.finer(() -> "Deserializing ordinal " + ordinal + " (wire=" + wireOrdinal + ")");
    
    if (ordinal < 0 || ordinal >= discoveredClasses.length) {
      throw new IllegalStateException("Invalid ordinal: " + ordinal + " (wire=" + wireOrdinal + ")");
    }
    
    Class<?> targetClass = discoveredClasses[ordinal];
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

  ByteBuffer allocateForWriting(int bytes) {
    ByteBuffer buffer = ByteBuffer.allocate(bytes);
    buffer.order(java.nio.ByteOrder.BIG_ENDIAN);
    return buffer;
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
                    LOGGER.finer(() -> "Component " + component.getName() + " discovered types: " + structure.types().stream().map(Class::getSimpleName).toList());
                    return structure.types().stream();
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

  /// TypeStructure record for analyzing generic types
  record TypeStructure(List<Tag> tags, List<Class<?>> types) {
    
    /// Analyze a generic type and extract its structure
    static TypeStructure analyze(Type type) {
      List<Tag> tags = new ArrayList<>();
      List<Class<?>> types = new ArrayList<>();
      
      Object current = type;
      
      while (current != null) {
        if (current instanceof ParameterizedType paramType) {
          Type rawType = paramType.getRawType();
          
          if (rawType.equals(java.util.List.class)) {
            tags.add(Tag.LIST);
            Type[] typeArgs = paramType.getActualTypeArguments();
            current = typeArgs.length > 0 ? typeArgs[0] : null;
            continue;
          } else if (rawType.equals(java.util.Map.class)) {
            tags.add(Tag.MAP);
            // For maps, we need to handle both key and value types, but for simplicity we'll skip for now
            return new TypeStructure(tags, types);
          } else if (rawType.equals(java.util.Optional.class)) {
            tags.add(Tag.OPTIONAL);
            Type[] typeArgs = paramType.getActualTypeArguments();
            current = typeArgs.length > 0 ? typeArgs[0] : null;
            continue;
          } else {
            // Unknown parameterized type, treat as raw type
            current = rawType;
            continue;
          }
        } else if (current instanceof Class<?> clazz) {
          if (clazz.isArray()) {
            // Handle array class like Person[].class
            tags.add(Tag.ARRAY);
            Class<?> componentType = clazz.getComponentType();
            current = componentType; // Continue with component type
            continue;
          } else {
            // Regular class - terminal case
            tags.add(Tag.fromClass(clazz));
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
}
