package io.github.simbo1905.no.framework;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.*;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static io.github.simbo1905.no.framework.Pickler.LOGGER;
import static io.github.simbo1905.no.framework.Tag.*;

record RecordReflection<R extends Record>(MethodHandle constructor, MethodHandle[] componentAccessors,
                                          TypeStructure[] componentTypes,
                                          BiConsumer<WriteBuffer, Object>[] componentWriters,
                                          Function<ByteBuffer, Object>[] componentReaders) {

  static <R extends Record> RecordReflection<R> analyze(Class<R> recordClass) throws Throwable {
    RecordComponent[] components = recordClass.getRecordComponents();
    MethodHandles.Lookup lookup = MethodHandles.lookup();

    // Constructor reflection
    Class<?>[] parameterTypes = Arrays.stream(components)
        .map(RecordComponent::getType)
        .toArray(Class<?>[]::new);
    Constructor<?> constructorHandle = recordClass.getDeclaredConstructor(parameterTypes);
    MethodHandle constructor = lookup.unreflectConstructor(constructorHandle);

    // Component accessors and type analysis
    MethodHandle[] componentAccessors = new MethodHandle[components.length];
    TypeStructure[] componentTypes = new TypeStructure[components.length];
    @SuppressWarnings("unchecked")
    BiConsumer<WriteBuffer, Object>[] componentWriters = new BiConsumer[components.length];
    @SuppressWarnings("unchecked")
    Function<ByteBuffer, Object>[] componentReaders = new Function[components.length];

    IntStream.range(0, components.length).forEach(i -> {
      RecordComponent component = components[i];
      // Create accessor
      try {
        componentAccessors[i] = lookup.unreflect(component.getAccessor());
      } catch (IllegalAccessException e) {
        throw new RuntimeException(e.getMessage(), e);
      }

      // Analyze type structure
      Type genericType = component.getGenericType();
      componentTypes[i] = TypeStructure.analyze(genericType);

      // Build writer and reader chains
      BiConsumer<WriteBuffer, Object> typeChain = Writers.buildWriterChain(componentTypes[i]);
      componentWriters[i] = createComponentExtractor(componentAccessors[i], typeChain);
      componentReaders[i] = Readers.buildReaderChain(componentTypes[i]);
    });

    return new RecordReflection<>(constructor, componentAccessors, componentTypes,
        componentWriters, componentReaders);
  }

  static BiConsumer<WriteBuffer, Object> createComponentExtractor(
      MethodHandle accessor,
      BiConsumer<WriteBuffer, Object> delegate) {
    return (buf, record) -> {
      Objects.requireNonNull(buf);
      Objects.requireNonNull(record);
      WriteBufferImpl bufImpl = (WriteBufferImpl) buf;
      ByteBuffer buffer = bufImpl.buffer;
      try {
        Object componentValue = accessor.invokeWithArguments(record);
        if (componentValue == null) {
          buffer.put(Constants.NULL.marker());
          LOGGER.fine(() -> "Writing NULL component");
        } else {
          delegate.accept(buf, componentValue);
        }
      } catch (Throwable e) {
        throw new RuntimeException("Failed to extract component", e);
      }
    };
  }

  @SuppressWarnings("unchecked")
  R deserialize(ByteBuffer buffer) throws Throwable {
    Object[] components = new Object[componentReaders.length];
    Arrays.setAll(components, i -> componentReaders[i].apply(buffer));
    return (R) constructor.invokeWithArguments(components);
  }

  void serialize(WriteBuffer writeBuffer, R record) {
    for (BiConsumer<WriteBuffer, Object> writer : componentWriters) {
      writer.accept(writeBuffer, record);
    }
  }
}

enum Tag {
  // Primitive types
  BOOLEAN(boolean.class, Boolean.class),
  BYTE(byte.class, Byte.class),
  SHORT(short.class, Short.class),
  CHARACTER(char.class, Character.class),
  INTEGER(int.class, Integer.class),
  LONG(long.class, Long.class),
  FLOAT(float.class, Float.class),
  DOUBLE(double.class, Double.class),
  STRING(String.class),

  // Container types
  OPTIONAL(Optional.class),
  LIST(List.class),
  MAP(Map.class),

  // Complex types
  ENUM(Enum.class),
  ARRAY(Arrays.class), // Arrays don't have a single class use Arrays.class as a marker
  RECORD(Record.class),
  UUID(java.util.UUID.class);

  final Class<?>[] supportedClasses;

  Tag(Class<?>... classes) {
    Objects.requireNonNull(classes);
    this.supportedClasses = classes;
  }

  static Tag fromClass(Class<?> clazz) {
    for (Tag tag : values()) {
      for (Class<?> supported : tag.supportedClasses) {
        if (supported.equals(clazz) || supported.isAssignableFrom(clazz)) {
          return tag;
        }
      }
    }
    // Special case for arrays
    if (clazz.isArray()) return ARRAY;
    throw new IllegalArgumentException("Unsupported class: " + clazz.getName());
  }
}

// TODO delete this and just use an array of tags
record TypeStructure(List<Tag> tags) {
  TypeStructure(List<Tag> tags) {
    this.tags = Collections.unmodifiableList(tags);
  }

  static TypeStructure analyze(Type type) {
    List<Tag> tags = new ArrayList<>();
    Type current = type;
    // TODO this is ugly can be made more stream oriented
    while (true) {
      if (current instanceof Class<?> clazz) {
        tags.add(Tag.fromClass(clazz));
        return new TypeStructure(tags);
      }

      if (current instanceof ParameterizedType paramType) {
        Type rawType = paramType.getRawType();
        Type[] typeArgs = paramType.getActualTypeArguments();

        if (rawType instanceof Class<?> rawClass) {
          Tag tag = Tag.fromClass(rawClass);
          tags.add(tag);

          // For containers, continue with the first type argument
          if (tag == Tag.LIST || tag == OPTIONAL) {
            current = typeArgs[0];
            continue;
          }

          if (tag == MAP) {
            final var keyType = typeArgs[0];
            if (keyType instanceof Class<?> keyClass) {
              tags.add(Tag.fromClass(keyClass));
            } else {
              throw new IllegalArgumentException("Unsupported map key type must be simple value type: " + keyType);
            }
            final var valueType = typeArgs[1];
            if (!(valueType instanceof Class<?>)) {
              throw new IllegalArgumentException("Unsupported map value type must be simple value type: " + valueType);
            }
            // For maps, we need special handling - for now just take value type
            current = valueType;
            continue;
          }
        }

        throw new IllegalArgumentException("Unsupported parameterized type: " + rawType);
      }

      if (current instanceof GenericArrayType arrayType) {
        tags.add(Tag.ARRAY);
        current = arrayType.getGenericComponentType();
        continue;
      }

      throw new IllegalArgumentException("Unsupported type: " + current);
    }
  }
}

final class Writers {
  static ByteBuffer byteBuffer(WriteBuffer buf, Object value) {
    Objects.requireNonNull(buf);
    Objects.requireNonNull(value);
    WriteBufferImpl bufImpl = (WriteBufferImpl) buf;
    return bufImpl.buffer;
  }

  static final BiConsumer<WriteBuffer, Object> BOOLEAN_WRITER = (buf, value) -> {
    ByteBuffer buffer = byteBuffer(buf, value);
    LOGGER.fine(() -> "Writing BOOLEAN - position=" + buffer.position() + " value=" + value);
    buffer.put(Constants.BOOLEAN.marker());
    buffer.put(((Boolean) value) ? (byte) 1 : (byte) 0);
  };

  static final BiConsumer<WriteBuffer, Object> BYTE_WRITER = (buf, value) -> {
    ByteBuffer buffer = byteBuffer(buf, value);
    LOGGER.fine(() -> "Writing BOOLEAN - position=" + buffer.position() + " value=" + value);
    buffer.put(Constants.BYTE.marker());
    buffer.put((Byte) value);
  };

  static final BiConsumer<WriteBuffer, Object> SHORT_WRITER = (buf, value) -> {
    ByteBuffer buffer = byteBuffer(buf, value);
    LOGGER.fine(() -> "Writing SHORT - position=" + buffer.position() + " value=" + value);
    buffer.put(Constants.SHORT.marker());
    buffer.putShort((Short) value);
  };

  static final BiConsumer<WriteBuffer, Object> CHAR_WRITER = (buf, value) -> {
    ByteBuffer buffer = byteBuffer(buf, value);
    LOGGER.fine(() -> "Writing CHARACTER - position=" + buffer.position() + " value=" + value);
    buffer.put(Constants.CHARACTER.marker());
    buffer.putChar((Character) value);
  };

  static final BiConsumer<WriteBuffer, Object> INTEGER_WRITER = (buf, value) -> {
    ByteBuffer buffer = byteBuffer(buf, value);
    LOGGER.fine(() -> "Writing INTEGER - position=" + buffer.position() + " value=" + value);
    buffer.put(Constants.INTEGER.marker()); // TODO zigzag encode
    buffer.putInt((Integer) value);
  };

  static final BiConsumer<WriteBuffer, Object> LONG_WRITER = (buf, value) -> {
    ByteBuffer buffer = byteBuffer(buf, value);
    LOGGER.fine(() -> "Writing LONG - position=" + buffer.position() + " value=" + value);
    buffer.put(Constants.LONG.marker()); // TODO zigzag encode
    buffer.putLong((Long) value);
  };

  static final BiConsumer<WriteBuffer, Object> FLOAT_WRITER = (buf, value) -> {
    ByteBuffer buffer = byteBuffer(buf, value);
    LOGGER.fine(() -> "Writing FLOAT - position=" + buffer.position() + " value=" + value);
    buffer.put(Constants.FLOAT.marker());
    buffer.putFloat((Float) value);
  };

  static final BiConsumer<WriteBuffer, Object> DOUBLE_WRITER = (buf, value) -> {
    ByteBuffer buffer = byteBuffer(buf, value);
    LOGGER.fine(() -> "Writing DOUBLE - position=" + buffer.position() + " value=" + value);
    buffer.put(Constants.DOUBLE.marker());
    buffer.putDouble((Double) value);
  };

  static final BiConsumer<WriteBuffer, Object> STRING_WRITER = (buf, value) -> {
    ByteBuffer buffer = byteBuffer(buf, value);
    LOGGER.fine(() -> "Writing STRING - position=" + buffer.position() + " value=" + value);
    buffer.put(Constants.STRING.marker());
    byte[] bytes = ((String) value).getBytes();
    buffer.putInt(bytes.length); // TODO zigzag encode
    buffer.put(bytes);
  };

  static final BiConsumer<WriteBuffer, Object> UUID_WRITER = (buf, value) -> {
    ByteBuffer buffer = byteBuffer(buf, value);
    LOGGER.fine(() -> "Writing ENUM - position=" + buffer.position() + " value=" + value);
    buffer.put(Constants.UUID.marker());
    java.util.UUID uuid = (java.util.UUID) value;
    buffer.putLong(uuid.getMostSignificantBits());
    buffer.putLong(uuid.getLeastSignificantBits());
  };

  static final BiConsumer<WriteBuffer, Object> ARRAY_WRITER = (buf, value) -> {
    ByteBuffer buffer = byteBuffer(buf, value);
    LOGGER.fine(() -> "Writing ARRAY - position=" + buffer.position() + " length=" + value);
    buffer.put(Constants.ARRAY.marker());
    switch (value) {
      case byte[] arr -> {
        buffer.put(Constants.BYTE.marker());
        ZigZagEncoding.putInt(buffer, Array.getLength(value));
        buffer.put(arr);
      }
      case boolean[] booleans -> {
        buffer.put(Constants.BOOLEAN.marker());
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
      }
      case int[] integers -> {
        buffer.put(Constants.INTEGER.marker());
        ZigZagEncoding.putInt(buffer, Array.getLength(value));
        for (int i : integers) {
          buffer.putInt(i);
        }
      }
      default -> throw new IllegalArgumentException("Unsupported array type: " + value.getClass());
    }
  };

  // Container writers
  static BiConsumer<WriteBuffer, Object> createDelegatingOptionalWriter(BiConsumer<WriteBuffer, Object> delegate) {
    return (buf, obj) -> {
      ByteBuffer buffer = byteBuffer(buf, obj);
      Optional<?> optional = (Optional<?>) obj;
      if (optional.isEmpty()) {
        LOGGER.fine(() -> "Writing OPTIONAL_EMPTY - position=" + buffer.position());
        buffer.put(Constants.OPTIONAL_EMPTY.marker());
      } else {
        LOGGER.fine(() -> "Writing OPTIONAL_OF - position=" + buffer.position() + " value=" + optional.get());
        buffer.put(Constants.OPTIONAL_OF.marker());
        delegate.accept(buf, optional.get());
      }
    };
  }

  static BiConsumer<WriteBuffer, Object> createDelegatingListWriter(BiConsumer<WriteBuffer, Object> delegate) {
    return (buf, obj) -> {
      ByteBuffer buffer = byteBuffer(buf, obj);
      List<?> list = (List<?>) obj;
      LOGGER.fine(() -> "Writing LIST - position=" + buffer.position() + " size=" + list.size());
      buffer.put(Constants.LIST.marker());
      buffer.putInt(list.size());
      for (Object item : list) {
        delegate.accept(buf, item);
      }
    };
  }

  static BiConsumer<WriteBuffer, Object> createMapWriter(BiConsumer<WriteBuffer, Object> keyDelegate,
                                                            BiConsumer<WriteBuffer, Object> valueDelegate) {
    return (buf, obj) -> {
      ByteBuffer buffer = byteBuffer(buf, obj);
      Map<?,?> map = (Map<?, ?>) obj;
      LOGGER.fine(() -> "Writing MAP - position=" + buffer.position() + " size=" + map.size());
      buffer.put(Constants.MAP.marker());
      ZigZagEncoding.putInt(buffer, map.size());
      for (Map.Entry<?, ?> entry : map.entrySet()) {
        keyDelegate.accept(buf, entry.getKey());
        valueDelegate.accept(buf, entry.getValue());
      }
    };
  }

  // Build writer chain from type structure
  static BiConsumer<WriteBuffer, Object> buildWriterChain(TypeStructure structure) {
    List<Tag> tags = structure.tags();
    if (tags.isEmpty()) {
      throw new IllegalArgumentException("Type structure must have at least one tag");
    }
    LOGGER.fine(() -> "Building writer chain for type structure: " +
        tags.stream().map(Enum::name).collect(Collectors.joining(",")));

    // Reverse the tags to process from right to left
    Iterator<Tag> reversedTags = tags.reversed().iterator();

    List<BiConsumer<WriteBuffer, Object>> writers = new ArrayList<>(tags.size());

    // Start with the leaf (rightmost) writer
    Tag rightmostTag = reversedTags.next();
    BiConsumer<WriteBuffer, Object> writer = createLeafWriter(rightmostTag);
    writers.add(writer);

    // For nested collection or option types we Build chain from right to left (reverse order)
    while (reversedTags.hasNext()) {
      final BiConsumer<WriteBuffer, Object> lastWriter = writer; // final required for lambda capture
      Tag tag = reversedTags.next();
      writer = switch (tag) {
        case LIST -> createDelegatingListWriter(lastWriter);
        case OPTIONAL -> createDelegatingOptionalWriter(lastWriter);
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

    return writer;
  }

  static BiConsumer<WriteBuffer, Object> createLeafWriter(Tag leafTag) {
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
      case ARRAY -> ARRAY_WRITER;
      case UUID -> UUID_WRITER;
      default -> throw new IllegalArgumentException("No leaf writer for tag: " + leafTag);
    };
  }
}

final class Readers {
  static final Function<ByteBuffer, Object> BOOLEAN_READER = (buffer) -> {
    LOGGER.fine(() -> "Reading BOOLEAN - position=" + buffer.position());
    byte marker = buffer.get();
    if (marker != Constants.BOOLEAN.marker()) {
      throw new IllegalStateException("Expected BOOLEAN marker but got: " + marker);
    }
    boolean value = buffer.get() != 0;
    LOGGER.finer(() -> "Read Boolean: " + value);
    return value;
  };

  static final Function<ByteBuffer, Object> BYTE_READER = (buffer) -> {
    LOGGER.fine(() -> "Reading BYTE - position=" + buffer.position());
    byte marker = buffer.get();
    if (marker != Constants.BYTE.marker()) {
      throw new IllegalStateException("Expected BYTE marker but got: " + marker);
    }
    byte value = buffer.get();
    LOGGER.finer(() -> "Read Byte: " + value);
    return value;
  };

  static final Function<ByteBuffer, Object> SHORT_READER = (buffer) -> {
    LOGGER.fine(() -> "Reading SHORT - position=" + buffer.position());
    byte marker = buffer.get();
    if (marker != Constants.SHORT.marker()) {
      throw new IllegalStateException("Expected SHORT marker but got: " + marker);
    }
    short value = buffer.getShort();
    LOGGER.finer(() -> "Read Short: " + value);
    return value;
  };

  static final Function<ByteBuffer, Object> CHAR_READER = (buffer) -> {
    LOGGER.fine(() -> "Reading CHARACTER - position=" + buffer.position());
    byte marker = buffer.get();
    if (marker != Constants.CHARACTER.marker()) {
      throw new IllegalStateException("Expected CHARACTER marker but got: " + marker);
    }
    char value = buffer.getChar();
    LOGGER.finer(() -> "Read Character: " + value);
    return value;
  };

  static final Function<ByteBuffer, Object> INTEGER_READER = (buffer) -> {
    LOGGER.fine(() -> "Reading INTEGER - position=" + buffer.position());
    byte marker = buffer.get();
    if (marker != Constants.INTEGER.marker()) {
      throw new IllegalStateException("Expected INTEGER marker but got: " + marker);
    }
    int value = buffer.getInt();
    LOGGER.finer(() -> "Read Integer: " + value);
    return value;
  };

  static final Function<ByteBuffer, Object> LONG_READER = (buffer) -> {
    LOGGER.fine(() -> "Reading LONG - position=" + buffer.position());
    byte marker = buffer.get();
    if (marker != Constants.LONG.marker()) {
      throw new IllegalStateException("Expected LONG marker but got: " + marker);
    }
    long value = buffer.getLong();
    LOGGER.finer(() -> "Read Long: " + value);
    return value;
  };

  static final Function<ByteBuffer, Object> FLOAT_READER = (buffer) -> {
    LOGGER.fine(() -> "Reading FLOAT - position=" + buffer.position());
    byte marker = buffer.get();
    if (marker != Constants.FLOAT.marker()) {
      throw new IllegalStateException("Expected FLOAT marker but got: " + marker);
    }
    float value = buffer.getFloat();
    LOGGER.finer(() -> "Read Float: " + value);
    return value;
  };

  static final Function<ByteBuffer, Object> DOUBLE_READER = (buffer) -> {
    LOGGER.fine(() -> "Reading DOUBLE - position=" + buffer.position());
    byte marker = buffer.get();
    if (marker != Constants.DOUBLE.marker()) {
      throw new IllegalStateException("Expected DOUBLE marker but got: " + marker);
    }
    double value = buffer.getDouble();
    LOGGER.finer(() -> "Read Double: " + value);
    return value;
  };

  static final Function<ByteBuffer, Object> STRING_READER = (buffer) -> {
    LOGGER.fine(() -> "Reading STRING - position=" + buffer.position());
    byte marker = buffer.get();
    if (marker != Constants.STRING.marker()) {
      throw new IllegalStateException("Expected STRING marker but got: " + marker);
    }
    int length = buffer.getInt();
    byte[] bytes = new byte[length];
    buffer.get(bytes);
    String value = new String(bytes);
    LOGGER.finer(() -> "Read String: " + value);
    return value;
  };

  static final Function<ByteBuffer, Object> UUID_READER = (buffer) -> {
    LOGGER.fine(() -> "Reading UUID - position=" + buffer.position());
    byte marker = buffer.get();
    if (marker != Constants.UUID.marker()) {
      throw new IllegalStateException("Expected UUID marker but got: " + marker);
    }
    final long mostSigBits = buffer.getLong();
    final long leastSigBits = buffer.getLong();
    UUID value = new UUID(mostSigBits, leastSigBits);
    LOGGER.finer(() -> "Read UUID: " + value);
    return value;
  };

  static final Function<ByteBuffer, Object> ARRAY_READER = (buffer) -> {
    LOGGER.fine(() -> "Reading ARRAY - position=" + buffer.position());
    byte marker = buffer.get();
    if (marker != Constants.ARRAY.marker()) {
      throw new IllegalStateException("Expected ARRAY marker but got: " + marker);
    }
    byte arrayTypeMarker = buffer.get();
    switch (Constants.fromMarker(arrayTypeMarker)) {
      case Constants.BYTE -> {
        int length = ZigZagEncoding.getInt(buffer);
        byte[] bytes = new byte[length];
        buffer.get(bytes);
        LOGGER.finer(() -> "Read Byte Array len=" + bytes.length);
        return bytes;
      }
      case Constants.BOOLEAN -> {
        int boolLength = ZigZagEncoding.getInt(buffer);
        LOGGER.finer(() -> "Read Boolean Array len=" + boolLength);
        boolean[] booleans = new boolean[boolLength];
        int bytesLength = ZigZagEncoding.getInt(buffer);
        LOGGER.finer(() -> "Read BetSet byte Array len=" + bytesLength);
        byte[] bytes = new byte[bytesLength];
        buffer.get(bytes);
        LOGGER.finer(() -> "Read BitSet bytes: " + Arrays.toString(bytes));
        BitSet bitSet = BitSet.valueOf(bytes);
        IntStream.range(0, boolLength).forEach(i -> {
          LOGGER.finer(() -> "Read BitSet " + i + "=" + bitSet.get(i));
          booleans[i] = bitSet.get(i);
        });
        return booleans;
      }
      case Constants.INTEGER -> {
        int length = ZigZagEncoding.getInt(buffer);
        LOGGER.finer(() -> "Read Integer Array len=" + length);
        int[] integers = new int[length];
        Arrays.setAll(integers, i -> buffer.getInt());
        return integers;
      }
      default -> throw new IllegalStateException("Unsupported array type marker: " + arrayTypeMarker);
    }
  };

  // Container readers
  static Function<ByteBuffer, Object> createDelegatingOptionalReader(Function<ByteBuffer, Object> delegate) {
    return (buffer) -> {
      LOGGER.fine(() -> "Reading OPTIONAL - position=" + buffer.position());
      byte marker = buffer.get();
      if (marker == Constants.OPTIONAL_EMPTY.marker()) {
        LOGGER.finer(() -> "Read OPTIONAL_EMPTY");
        return Optional.empty();
      } else if (marker == Constants.OPTIONAL_OF.marker()) {
        Object value = delegate.apply(buffer);
        LOGGER.finer(() -> "Read OPTIONAL_OF with value: " + value);
        return Optional.of(value);
      } else {
        throw new IllegalStateException("Expected OPTIONAL marker but got: " + marker);
      }
    };
  }

  static Function<ByteBuffer, Object> createDelegatingListReader(Function<ByteBuffer, Object> delegate) {
    return (buffer) -> {
      LOGGER.fine(() -> "Reading LIST - position=" + buffer.position());
      byte marker = buffer.get();
      if (marker != Constants.LIST.marker()) {
        throw new IllegalStateException("Expected LIST marker but got: " + marker);
      }
      int size = buffer.getInt();
      LOGGER.finer(() -> "Reading List with " + size + " elements");
      List<Object> list = new ArrayList<>(size);
      IntStream.range(0, size).forEach(i -> list.add(delegate.apply(buffer)));
      return Collections.unmodifiableList(list);
    };
  }

  static Function<ByteBuffer, Object> createMapReader(Function<ByteBuffer, Object> keyDelegate,
                                                      Function<ByteBuffer, Object> valueDelegate) {
    return (buffer) -> {
      Objects.requireNonNull(buffer);
      final var initialPosition = buffer.position();
      final var marker = buffer.get();
      if (marker != Constants.MAP.marker()) {
        throw new IllegalStateException("Expected MAP marker at position=" + initialPosition + " but got: " + marker);
      }
      int size = ZigZagEncoding.getInt(buffer);
      LOGGER.fine(() -> "Reading MAP - position=" + initialPosition + " size=" + size);
      Map<Object, Object> map = new LinkedHashMap<>(size);
      IntStream.range(0, size)
          .forEach(i -> {
            Object key = keyDelegate.apply(buffer);
            Object value = valueDelegate.apply(buffer);
            map.put(key, value);
          });
      return Collections.unmodifiableMap(map);
    };
  }

  // Get base reader for a tag
  static Function<ByteBuffer, Object> createLeafReader(Tag tag) {
    LOGGER.fine(() -> "Creating leaf reader for tag: " + tag);
    return switch (tag) {
      case BOOLEAN -> BOOLEAN_READER;
      case BYTE -> BYTE_READER;
      case SHORT -> SHORT_READER;
      case CHARACTER -> CHAR_READER;
      case INTEGER -> INTEGER_READER;
      case LONG -> LONG_READER;
      case FLOAT -> FLOAT_READER;
      case DOUBLE -> DOUBLE_READER;
      case STRING -> STRING_READER;
      case ARRAY -> ARRAY_READER;
      case UUID -> UUID_READER;
      default -> throw new IllegalArgumentException("No base reader for tag: " + tag);
    };
  }

  // Build reader chain from type structure
  static Function<ByteBuffer, Object> buildReaderChain(TypeStructure structure) {
    Objects.requireNonNull(structure);
    final var tags = structure.tags();
    if (tags == null || tags.isEmpty()) {
      throw new IllegalArgumentException("Type structure.tags() must have at least one tag: " + tags);
    }
    // Reverse the tags to process from right to left
    final var tagsIterator = tags.reversed().iterator();
    // To handle maps we need to look at the prior two tags in reverse order
    List<Function<ByteBuffer, Object>> readers = new ArrayList<>(tags.size());

    // Start with the leaf (rightmost) reader
    Function<ByteBuffer, Object> reader = createLeafReader(tagsIterator.next());
    readers.add(reader);

    // Build chain from right to left (reverse order)
    while (tagsIterator.hasNext()) {
      final Function<ByteBuffer, Object> delegateToReader = reader; // final required for lambda capture
      Tag preceedingTag = tagsIterator.next();
      reader = switch (preceedingTag) {
        case LIST -> createDelegatingListReader(delegateToReader);
        case OPTIONAL -> createDelegatingOptionalReader(delegateToReader);
        case MAP -> // as we are going in reverse order it is
            createMapReader(readers.getLast(), readers.get(readers.size() - 2));
        default -> createLeafReader(preceedingTag);
      };
      readers.add(reader);
    }

    return reader;
  }
}
