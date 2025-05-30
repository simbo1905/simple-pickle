// SPDX-FileCopyrightText: 2025 Simon Massey
// SPDX-License-Identifier: MIT

package io.github.simbo1905.no.framework;

import java.nio.ByteBuffer;
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
  
  // Combined class name mappings from all delegatee RecordPicklers
  final ClassNameMappings combinedClassNameMappings;

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
    
    // Collect ClassNameMappings from all delegatee RecordPicklers and merge them
    ClassNameMappings[] mappingsArray = subPicklers.values().stream()
        .filter(pickler -> pickler instanceof RecordPickler<?>)
        .map(pickler -> ((RecordPickler<?>) pickler).getClassNameMappings())
        .toArray(ClassNameMappings[]::new);
    
    this.combinedClassNameMappings = ClassNameMappings.merge(mappingsArray);
  }

  /// Here we simply delegate to the RecordPickler which is configured to first write out its name.
  @Override
  public int serialize(WriteBuffer buffer, S object) {
    Objects.requireNonNull(buffer, "buffer");
    if (buffer.isClosed()) {
      throw new IllegalStateException("Cannot serialize to a closed buffer");
    }
    final var buf = (WriteBufferImpl) buffer;
    final var startPosition = buf.position();
    if (object == null) {
      buf.put(NULL.marker());
      return 1; // 1 byte for NULL marker
    }
    //noinspection unchecked
    Class<? extends S> concreteType = (Class<? extends S>) object.getClass();
    Pickler<?> pickler = subPicklers.get(concreteType);

    // Use shared class name compression logic
    Writers.writeCompressedClassName(buf, object.getClass());

    Companion.serializeWithPickler(buf, pickler, object);
    return buf.position() - startPosition;
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
    buffer.reset();
    buf.nameToClass.putAll(nameToRecordClass);
    // Use shared class name decompression logic
    Class<?> clazz = Writers.readCompressedClassName(buf);
    final RecordPickler<?> pickler = (RecordPickler<?>) subPicklers.get(clazz);
    if (pickler == null) {
      throw new IllegalStateException("No pickler found for " + clazz.getName() + " in sealed hierarchy: " +
          String.join(",", this.recordClassByName.keySet()));
    }
    try {
      //noinspection unchecked
      return (S) pickler.deserialize(buf);
    } catch (RuntimeException e) {
      throw e;
    } catch (Throwable t) {
      throw new RuntimeException("Failed to deserialize " + clazz.getName() + " : " + t.getMessage(), t);
    }
  }

  @Override
  public WriteBuffer allocateForWriting(int size) {
    return new WriteBufferImpl(
        ByteBuffer.allocate(size),
        combinedClassNameMappings.classToInternedName()::get
    );
  }

  @Override
  public WriteBuffer wrapForWriting(ByteBuffer buf) {
    return new WriteBufferImpl(
        buf,
        combinedClassNameMappings.classToInternedName()::get
    );
  }

  @Override
  public int maxSizeOf(S record) {
    if (record == null) {
      return 1; // NULL marker
    }
    @SuppressWarnings("unchecked")
    Class<? extends S> concreteType = (Class<? extends S>) record.getClass();
    Pickler<? extends S> pickler = subPicklers.get(concreteType);
    if (pickler == null) {
      throw new IllegalArgumentException("No pickler found for type: " + concreteType.getName());
    }
    // Delegate to the concrete pickler's maxSizeOf method
    @SuppressWarnings("unchecked")
    Pickler<S> typedPickler = (Pickler<S>) pickler;
    return typedPickler.maxSizeOf(record);
  }

  @Override
  public ReadBuffer allocateForReading(int size) {
    return new ReadBufferImpl(
        ByteBuffer.allocate(size),
        combinedClassNameMappings.shortNameToClass()::get
    );
  }

  @Override
  public ReadBuffer wrapForReading(ByteBuffer buf) {
    return new ReadBufferImpl(
        buf,
        combinedClassNameMappings.shortNameToClass()::get
    );
  }
}
