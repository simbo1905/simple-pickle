package io.github.simbo1905.no.framework;

import java.lang.reflect.RecordComponent;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static io.github.simbo1905.no.framework.Companion.nameToBasicClass;

final class RecordPickler<R extends Record> implements Pickler<R> {

  final RecordReflection<R> reflection;
  final Map<String, Class<?>> nameToClass = new HashMap<>(nameToBasicClass);
  final Map<Enum<?>, InternedName> enumToName = new HashMap<>();
  final Map<String, Enum<?>> nameToEnum;
  private final Class<R> recordClass;

  public RecordPickler(final Class<R> recordClass,
                       InternedName internedName,
                       final Map<String, Class<?>> classesByShortName) {
    Objects.requireNonNull(recordClass);
    Objects.requireNonNull(internedName);
    Objects.requireNonNull(classesByShortName);
    if (!recordClass.isRecord()) {
      throw new IllegalArgumentException("Class " + recordClass.getName() + " is not a record");
    }
    this.recordClass = recordClass;
    final RecordComponent[] components = recordClass.getRecordComponents();
    reflection = RecordReflection.analyze(recordClass);
    
    TypeStructure[] componentTypes = new TypeStructure[components.length];
    IntStream.range(0, components.length).forEach(i -> {
          RecordComponent component = components[i];
          // Analyze type structure
          Type genericType = component.getGenericType();
          componentTypes[i] = TypeStructure.analyze(genericType);
        });

    // Get parameter types for the canonical constructor
    Class<?>[] parameterTypes = Arrays.stream(components).map(RecordComponent::getType).toArray(Class<?>[]::new);

    // Record the regular types, the array types, and the enums that we will write into the buffer
    Arrays.stream(parameterTypes).forEach(c -> {
      switch (c) {
        case Class<?> arrayType when arrayType.isArray() -> {
          Class<?> componentType = arrayType.getComponentType();
          nameToClass.putIfAbsent(componentType.getName(), componentType); // FIXME: make short name
        }
        case Class<?> enumType when enumType.isEnum() -> {
          for (Object e : enumType.getEnumConstants()) {
            if (e instanceof Enum<?> enumConst) {
              final var shortName = IntStream.range(0, Math.min(enumType.getName().length(), recordClass.getName().length()))
                  .takeWhile(i -> enumType.getName().charAt(i) == recordClass.getName().charAt(i))
                  .reduce((a, b) -> b)
                  .stream()
                  .mapToObj(i -> enumType.getName().substring(i + 1) + "." + enumConst.name())
                  .findFirst()
                  .orElse(enumType.getName() + "." + enumConst.name());
              enumToName.put(enumConst, new InternedName(shortName));
            }
          }
        }
        default -> nameToClass.putIfAbsent(c.getName(), c); // FIXME make shortName
      }
    });

    // flip the map for the deserialization
    nameToEnum = enumToName.entrySet().stream()
        .collect(Collectors.toMap(e -> e.getValue().name(), Map.Entry::getKey));
  }

  @Override
  public int serialize(WriteBuffer buffer, R object) {
    // Validations
    Objects.requireNonNull(buffer);
    if (buffer.isClosed()) {
      throw new IllegalStateException("WriteBuffer is closed");
    }
    if (0 == object.getClass().getRecordComponents().length) {
      LOGGER.fine(() -> object.getClass().getName() + " has no components. Built-in collections conversion to arrays may cause this problem.");
    }
    final var buf = ((WriteBufferImpl) buffer);
    final var startPos = buf.position();
    // Ensure java native endian writes
    buf.buffer.order(java.nio.ByteOrder.BIG_ENDIAN);
    // Initialize the state tracking the offsets of the record class names
    buf.enumToName.putAll(enumToName);
    buf.nameToClass.putAll(nameToClass);
    this.reflection.serialize(buf, object);
    return buffer.position() - startPos;
  }

  @Override
  public R deserialize(ReadBuffer readBuffer) {
    Objects.requireNonNull(readBuffer);
    if (readBuffer.isClosed()) {
      throw new IllegalStateException("PackedBuffer is closed");
    }
    final var buf = ((ReadBufferImpl) readBuffer);
    final var buffer = buf.buffer;
    buf.buffer.order(java.nio.ByteOrder.BIG_ENDIAN);
    buf.nameToEnum.putAll(nameToEnum);
    buf.nameToClass.putAll(nameToClass);
    try {
      return this.reflection.deserialize(buffer);
    } catch (RuntimeException e) {
      throw e;
    } catch (Throwable t) {
      throw new RuntimeException("Failed to deserialize " + recordClass.getName() + " : " + t.getMessage(), t);
    }
  }

  @Override
  public WriteBuffer allocateSufficient(R record) {
    int maxSize = reflection.maxSize(record);
    return WriteBuffer.of(maxSize);
  }

  public R deserializeWithMap(ReadBufferImpl buf, boolean writeName) throws Throwable {
    Objects.requireNonNull(buf);
    if (buf.isClosed()) {
      throw new IllegalStateException("PackedBuffer is closed");
    }
    final var buffer = buf.buffer;
    buffer.mark();
    final byte marker = buffer.get();
    if (marker == Constants.NULL.marker()) {
      return null;
    }
    if (marker != Constants.INTERNED_NAME.marker() && marker != Constants.INTERNED_OFFSET.marker()) {
      throw new IllegalStateException("Expected marker byte for INTERNED_NAME("
          + Constants.INTERNED_NAME.marker() + ") or INTERNED_OFFSET("
          + Constants.INTERNED_OFFSET.marker() + ") for INTERNED_NAME but got "
          + marker);
    }
    buffer.reset();
    // Read the interned name
    final InternedName name = (InternedName) Companion.read(-1, buf); // TODO annoying that passing -1 for unused
    assert name != null;
    if (!recordClass.getName().equals(name.name())) {
      throw new IllegalStateException("Expected record class name " + recordClass.getName() + " but got " + name.name());
    }
    return reflection.deserialize(buffer);
  }
}
