package io.github.simbo1905.no.framework;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import static io.github.simbo1905.no.framework.Companion.nameToBasicClass;
import static io.github.simbo1905.no.framework.Constants.*;

class SealedPickler<S> implements Pickler<S> {
  final Map<Class<? extends S>, Pickler<? extends S>> subPicklers;
  final Map<String, Class<?>> classesByShortName;
  final Map<String, Class<?>> nameToRecordClass = new HashMap<>(nameToBasicClass);

  public SealedPickler(
      Map<Class<? extends S>, Pickler<? extends S>> subPicklers,
      Map<String, Class<?>> classesByShortName) {
    this.subPicklers = subPicklers;
    this.classesByShortName = classesByShortName;
    this.nameToRecordClass.putAll(classesByShortName.entrySet().stream()
        .filter(e -> e.getValue().isRecord())
        .collect(Collectors.toMap(
            Map.Entry::getKey,
            Map.Entry::getValue
        )));
  }

  /// Here we simply delegate to the RecordPickler which is configured to first write out its name.
  @Override
  public void serialize(PackedBuffer buffer, S object) {
    final var buf = (PackedBufferImpl) buffer;
    if (object == null) {
      buf.put(NULL.marker());
      return;
    }
    //noinspection unchecked
    Class<? extends S> concreteType = (Class<? extends S>) object.getClass();
    Pickler<?> pickler = subPicklers.get(concreteType);
    Companion.serializeWithPickler(buf, pickler, object);
  }

  @Override
  public S deserialize(ByteBuffer buffer) {
    buffer.order(java.nio.ByteOrder.BIG_ENDIAN);
    buffer.mark();
    final byte marker = buffer.get();
    if (marker == NULL.marker()) {
      return null;
    }
    if (marker != INTERNED_NAME.marker() && marker != INTERNED_OFFSET.marker()) {
      throw new IllegalStateException("Expected marker byte for INTERNED_NAME("
          + INTERNED_NAME.marker() + ") or INTERNED_OFFSET("
          + INTERNED_OFFSET.marker() + ") for INTERNED_NAME but got "
          + marker);
    }
    buffer.reset();
    // Read the interned name
    final InternedName name = (InternedName) Companion.read(nameToRecordClass, buffer);
    assert name != null;
    final RecordPickler<?> pickler = (RecordPickler<?>) subPicklers.get(classesByShortName.get(name.name()));
    if (pickler == null) {
      throw new IllegalStateException("No pickler found for " + name.name());
    }
    try {
      //noinspection unchecked
      return (S) pickler.deserializeWithMap(pickler.nameToClass, buffer, false);
    } catch (RuntimeException e) {
      throw e;
    } catch (Throwable t) {
      throw new RuntimeException("Failed to deserialize " + name.name() + " : " + t.getMessage(), t);
    }
  }

  @Override
  public int sizeOf(S record) {
    //noinspection unchecked
    Class<? extends S> concreteType = (Class<? extends S>) record.getClass();
    Pickler<?> pickler = subPicklers.get(concreteType);
    return Companion.sizeOf(pickler, record);
  }
}
