// SPDX-FileCopyrightText: 2025 Simon Massey
// SPDX-License-Identifier: MIT

package io.github.simbo1905.no.framework;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static io.github.simbo1905.no.framework.Companion.nameToBasicClass;
import static io.github.simbo1905.no.framework.Constants.*;

class SealedPickler<S> implements Pickler<S> {
  final Map<Class<? extends S>, Pickler<? extends S>> subPicklers;
  final Map<String, Class<?>> recordClassByName;
  final Map<String, Class<?>> nameToRecordClass = new HashMap<>(nameToBasicClass);

  public SealedPickler(
      Map<Class<? extends S>, Pickler<? extends S>> subPicklers,
      Map<String, Class<?>> classesByShortName) {
    this.subPicklers = subPicklers;
    this.recordClassByName = classesByShortName;
    this.nameToRecordClass.putAll(classesByShortName.entrySet().stream()
        .filter(e -> e.getValue().isRecord())
        .collect(Collectors.toMap(
            Map.Entry::getKey,
            Map.Entry::getValue
        )));
  }

  /// Here we simply delegate to the RecordPickler which is configured to first write out its name.
  @Override
  public int serialize(WriteBuffer buffer, S object) {
    Objects.requireNonNull(buffer, "buffer");
    if (buffer.isClosed()) {
      throw new IllegalStateException("Cannot serialize to a closed buffer");
    }
    final var buf = (WriteBufferImpl) buffer;
    if (object == null) {
      buf.put(NULL.marker());
      return 1; // 1 byte for NULL marker
    }
    //noinspection unchecked
    Class<? extends S> concreteType = (Class<? extends S>) object.getClass();
    Pickler<?> pickler = subPicklers.get(concreteType);
    return Companion.serializeWithPickler(buf, pickler, object);
  }

  @Override
  public S deserialize(ReadBuffer readBuffer) {
    final var buf = (ReadBufferImpl) readBuffer;
    final var buffer = buf.buffer;
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
    buf.nameToClass.putAll(nameToRecordClass);
    // Read the interned name
    final InternedName name = (InternedName) Companion.read(-1, buf); // TODO annoying that passing -1 for unused
    assert name != null;
    final RecordPickler<?> pickler = (RecordPickler<?>) subPicklers.get(recordClassByName.get(name.name()));
    if (pickler == null) {
      throw new IllegalStateException("No pickler found for " + name.name());
    }
    buf.nameToClass.clear();
    buf.nameToClass.putAll(pickler.nameToClass);
    try {
      //noinspection unchecked
      return (S) pickler.deserializeWithMap(buf, false);
    } catch (RuntimeException e) {
      throw e;
    } catch (Throwable t) {
      throw new RuntimeException("Failed to deserialize " + name.name() + " : " + t.getMessage(), t);
    }
  }
}
