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
import static io.github.simbo1905.no.framework.Tag.MAP;
import static io.github.simbo1905.no.framework.Tag.OPTIONAL;
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
  RECORD(Record.class);

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

record TypeStructure(List<Tag> tags, Type leafType) {
  TypeStructure(List<Tag> tags, Type leafType) {
    this.tags = Collections.unmodifiableList(tags);
    this.leafType = leafType;
  }

  static TypeStructure analyze(Type type) {
    List<Tag> tags = new ArrayList<>();
    Type current = type;
    // TODO this is ugly can be made more stream oriented
    while (true) {
      if (current instanceof Class<?> clazz) {
        tags.add(Tag.fromClass(clazz));
        return new TypeStructure(tags, current);
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
            // For maps, we need special handling - for now just take value type
            current = typeArgs[1];
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
    LOGGER.fine(() -> "Writing BOOLEAN - position=" +buffer.position() + " value=" + value);
    buffer.put(Constants.BOOLEAN.marker());
    buffer.put(((Boolean) value) ? (byte) 1 : (byte) 0);
  };

  static final BiConsumer<WriteBuffer, Object> BYTE_WRITER = (buf, value) -> {
    ByteBuffer buffer = byteBuffer(buf, value);
    LOGGER.fine(() -> "Writing BOOLEAN - position=" +buffer.position() + " value=" + value);
    buffer.put(Constants.BYTE.marker());
    buffer.put((Byte) value);
  };

  static final BiConsumer<WriteBuffer, Object> SHORT_WRITER = (buf, value) -> {
    ByteBuffer buffer = byteBuffer(buf, value);
    LOGGER.fine(() -> "Writing SHORT - position=" +buffer.position() + " value=" + value);
    buffer.put(Constants.SHORT.marker());
    buffer.putShort((Short) value);
  };

  static final BiConsumer<WriteBuffer, Object> CHAR_WRITER = (buf, value) -> {
    ByteBuffer buffer = byteBuffer(buf, value);
    LOGGER.fine(() -> "Writing CHARACTER - position=" +buffer.position() + " value=" + value);
    buffer.put(Constants.CHARACTER.marker());
    buffer.putChar((Character) value);
  };

  static final BiConsumer<WriteBuffer, Object> INTEGER_WRITER = (buf, value) -> {
    ByteBuffer buffer = byteBuffer(buf, value);
    LOGGER.fine(() -> "Writing INTEGER - position=" +buffer.position() + " value=" + value);
    buffer.put(Constants.INTEGER.marker());
    buffer.putInt((Integer) value);
  };

  static final BiConsumer<WriteBuffer, Object> LONG_WRITER = (buf, value) -> {
    ByteBuffer buffer = byteBuffer(buf, value);
    LOGGER.fine(() -> "Writing LONG - position=" +buffer.position() + " value=" + value);
    buffer.put(Constants.LONG.marker());
    buffer.putLong((Long) value);
  };

  static final BiConsumer<WriteBuffer, Object> FLOAT_WRITER = (buf, value) -> {
    ByteBuffer buffer = byteBuffer(buf, value);
    LOGGER.fine(() -> "Writing FLOAT - position=" +buffer.position() + " value=" + value);
    buffer.put(Constants.FLOAT.marker());
    buffer.putFloat((Float) value);
  };

  static final BiConsumer<WriteBuffer, Object> DOUBLE_WRITER = (buf, value) -> {
    ByteBuffer buffer = byteBuffer(buf, value);
    LOGGER.fine(() -> "Writing DOUBLE - position=" +buffer.position() + " value=" + value);
    buffer.put(Constants.DOUBLE.marker());
    buffer.putDouble((Double) value);
  };

  static final BiConsumer<WriteBuffer, Object> STRING_WRITER = (buf, value) -> {
    ByteBuffer buffer = byteBuffer(buf, value);
    LOGGER.fine(() -> "Writing STRING - position=" +buffer.position() + " value=" + value);
    buffer.put(Constants.STRING.marker());
    byte[] bytes = ((String) value).getBytes();
    buffer.putInt(bytes.length);
    buffer.put(bytes);
  };

  static final BiConsumer<WriteBuffer, Object> ARRAY_WRITER = (buf, value) -> {
    ByteBuffer buffer = byteBuffer(buf, value);
    LOGGER.fine(() -> "Writing ARRAY - position=" +buffer.position() + " length=" + value);
    buffer.put(Constants.ARRAY.marker());
    switch (value){
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
      default -> throw new IllegalArgumentException("Unsupported array type: " + value.getClass());
    }
  };

  // Container writers
  static BiConsumer<WriteBuffer, Optional<?>> createOptionalWriter(BiConsumer<WriteBuffer, Object> delegate) {
    return (buf, optional) -> {
      ByteBuffer buffer = byteBuffer(buf, optional);
      if (optional.isEmpty()) {
        LOGGER.fine(() -> "Writing OPTIONAL_EMPTY - position=" +buffer.position());
        buffer.put(Constants.OPTIONAL_EMPTY.marker());
      } else {
        LOGGER.fine(() -> "Writing OPTIONAL_OF - position=" +buffer.position() + " value=" + optional.get());
        buffer.put(Constants.OPTIONAL_OF.marker());
        delegate.accept(buf, optional.get());
      }
    };
  }

  static BiConsumer<WriteBuffer, List<?>> createListWriter(BiConsumer<WriteBuffer, Object> delegate) {
    return (buf, list) -> {
      ByteBuffer buffer = byteBuffer(buf, list);
      LOGGER.fine(() -> "Writing LIST - position=" +buffer.position() + " size=" + list.size());
      buffer.put(Constants.LIST.marker());
      buffer.putInt(list.size());
      for (Object item : list) {
        delegate.accept(buf, item);
      }
    };
  }

  static BiConsumer<WriteBuffer, Map<?, ?>> createMapWriter(BiConsumer<WriteBuffer, Object> keyDelegate,
                                                            BiConsumer<WriteBuffer, Object> valueDelegate) {
    return (buf, map) -> {
      ByteBuffer buffer = byteBuffer(buf, map);
      LOGGER.fine(() -> "Writing MAP - position=" +buffer.position() + " size=" + map.size());
      buffer.put(Constants.MAP.marker());
      buffer.putInt(map.size());
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

    // Start with the leaf (rightmost) writer
    Tag rightmostTag = reversedTags.next();
    BiConsumer<WriteBuffer, Object> writer = switch (rightmostTag) {
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
      default -> throw new IllegalArgumentException("No leaf writer for tag: " + rightmostTag);
    };

    // For nested collection or option types we Build chain from right to left (reverse order)
    while (reversedTags.hasNext()) {
      Tag tag = reversedTags.next();
      BiConsumer<WriteBuffer, Object> currentWriter = writer;
      writer = switch (tag) {
        case LIST -> (buf, obj) ->
            createListWriter(currentWriter).accept(buf, (List<?>) obj);
        case OPTIONAL -> (buf, obj) ->
            createOptionalWriter(currentWriter).accept(buf, (Optional<?>) obj);
        case MAP -> throw new UnsupportedOperationException("Map writer chain not yet implemented");
        default -> throw new IllegalArgumentException("Unsupported container tag: " + tag);
      };
    }

    return writer;
  }
}

final class Readers {
  static final Function<ByteBuffer, Boolean> BOOLEAN_READER = (buffer) -> {
    LOGGER.fine(() -> "Reading BOOLEAN - position=" + buffer.position());
    byte marker = buffer.get();
    if (marker != Constants.BOOLEAN.marker()) {
      throw new IllegalStateException("Expected BOOLEAN marker but got: " + marker);
    }
    boolean value = buffer.get() != 0;
    LOGGER.finer(() -> "Read Boolean: " + value);
    return value;
  };

  static final Function<ByteBuffer, Byte> BYTE_READER = (buffer) -> {
    LOGGER.fine(() -> "Reading BYTE - position=" + buffer.position());
    byte marker = buffer.get();
    if (marker != Constants.BYTE.marker()) {
      throw new IllegalStateException("Expected BYTE marker but got: " + marker);
    }
    byte value = buffer.get();
    LOGGER.finer(() -> "Read Byte: " + value);
    return value;
  };

  static final Function<ByteBuffer, Short> SHORT_READER = (buffer) -> {
    LOGGER.fine(() -> "Reading SHORT - position=" + buffer.position());
    byte marker = buffer.get();
    if (marker != Constants.SHORT.marker()) {
      throw new IllegalStateException("Expected SHORT marker but got: " + marker);
    }
    short value = buffer.getShort();
    LOGGER.finer(() -> "Read Short: " + value);
    return value;
  };

  static final Function<ByteBuffer, Character> CHAR_READER = (buffer) -> {
    LOGGER.fine(() -> "Reading CHARACTER - position=" + buffer.position());
    byte marker = buffer.get();
    if (marker != Constants.CHARACTER.marker()) {
      throw new IllegalStateException("Expected CHARACTER marker but got: " + marker);
    }
    char value = buffer.getChar();
    LOGGER.finer(() -> "Read Character: " + value);
    return value;
  };

  static final Function<ByteBuffer, Integer> INTEGER_READER = (buffer) -> {
    LOGGER.fine(() -> "Reading INTEGER - position=" + buffer.position());
    byte marker = buffer.get();
    if (marker != Constants.INTEGER.marker()) {
      throw new IllegalStateException("Expected INTEGER marker but got: " + marker);
    }
    int value = buffer.getInt();
    LOGGER.finer(() -> "Read Integer: " + value);
    return value;
  };

  static final Function<ByteBuffer, Long> LONG_READER = (buffer) -> {
    LOGGER.fine(() -> "Reading LONG - position=" + buffer.position());
    byte marker = buffer.get();
    if (marker != Constants.LONG.marker()) {
      throw new IllegalStateException("Expected LONG marker but got: " + marker);
    }
    long value = buffer.getLong();
    LOGGER.finer(() -> "Read Long: " + value);
    return value;
  };

  static final Function<ByteBuffer, Float> FLOAT_READER = (buffer) -> {
    LOGGER.fine(() -> "Reading FLOAT - position=" + buffer.position());
    byte marker = buffer.get();
    if (marker != Constants.FLOAT.marker()) {
      throw new IllegalStateException("Expected FLOAT marker but got: " + marker);
    }
    float value = buffer.getFloat();
    LOGGER.finer(() -> "Read Float: " + value);
    return value;
  };

  static final Function<ByteBuffer, Double> DOUBLE_READER = (buffer) -> {
    LOGGER.fine(() -> "Reading DOUBLE - position=" + buffer.position());
    byte marker = buffer.get();
    if (marker != Constants.DOUBLE.marker()) {
      throw new IllegalStateException("Expected DOUBLE marker but got: " + marker);
    }
    double value = buffer.getDouble();
    LOGGER.finer(() -> "Read Double: " + value);
    return value;
  };

  static final Function<ByteBuffer, String> STRING_READER = (buffer) -> {
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

  static final Function<ByteBuffer, Object> ARRAY_READER = (buffer) -> {
    LOGGER.fine(() -> "Reading ARRAY - position=" + buffer.position());
    byte marker = buffer.get();
    if (marker != Constants.ARRAY.marker()) {
      throw new IllegalStateException("Expected ARRAY marker but got: " + marker);
    }
    byte arrayTypeMarker = buffer.get();
    switch(Constants.fromMarker(arrayTypeMarker)){
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
      default -> throw new IllegalStateException("Unsupported array type marker: " + arrayTypeMarker);
    }
  };

  // Container readers
  static Function<ByteBuffer, Optional<?>> createOptionalReader(Function<ByteBuffer, Object> delegate) {
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

  static Function<ByteBuffer, List<?>> createListReader(Function<ByteBuffer, Object> delegate) {
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

  static Function<ByteBuffer, Map<?, ?>> createMapReader(Function<ByteBuffer, Object> keyDelegate,
                                                         Function<ByteBuffer, Object> valueDelegate) {
    return (buffer) -> {
      LOGGER.fine(() -> "Reading MAP - position=" + buffer.position());
      byte marker = buffer.get();
      if (marker != Constants.MAP.marker()) {
        throw new IllegalStateException("Expected MAP marker but got: " + marker);
      }
      int size = buffer.getInt();
      LOGGER.fine(() -> "Reading Map with " + size + " entries");
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
  static Function<ByteBuffer, Object> innerReader(Tag tag) {
    return switch (tag) {
      case BOOLEAN -> BOOLEAN_READER::apply;
      case BYTE -> BYTE_READER::apply;
      case SHORT -> SHORT_READER::apply;
      case CHARACTER -> CHAR_READER::apply;
      case INTEGER -> INTEGER_READER::apply;
      case LONG -> LONG_READER::apply;
      case FLOAT -> FLOAT_READER::apply;
      case DOUBLE -> DOUBLE_READER::apply;
      case STRING -> STRING_READER::apply;
      case ARRAY -> ARRAY_READER::apply;
      default -> throw new IllegalArgumentException("No base reader for tag: " + tag);
    };
  }

  // Build reader chain from type structure
  static Function<ByteBuffer, Object> buildReaderChain(TypeStructure structure) {
    // Reverse the tags to process from right to left
    final var tagsIterator = structure.tags().reversed().iterator();

    // Start with the leaf (rightmost) reader
    Function<ByteBuffer, Object> reader = innerReader(tagsIterator.next());

    // Build chain from right to left (reverse order)
    while (tagsIterator.hasNext()) {
      Tag tag = tagsIterator.next();
      Function<ByteBuffer, Object> currentReader = reader;

      reader = switch (tag) {
        case LIST -> (buffer) -> createListReader(currentReader).apply(buffer);
        case OPTIONAL -> (buffer) -> createOptionalReader(currentReader).apply(buffer);
        case MAP -> throw new UnsupportedOperationException("Map reader chain not yet implemented");
        default -> throw new IllegalArgumentException("Unsupported container tag: " + tag);
      };
    }

    return reader;
  }
}
