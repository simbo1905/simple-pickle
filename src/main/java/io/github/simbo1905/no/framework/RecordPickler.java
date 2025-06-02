package io.github.simbo1905.no.framework;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static io.github.simbo1905.no.framework.Pickler.LOGGER;

final class RecordPickler<R extends Record> {

  final RecordReflection<R> reflection;
  private final Class<R> recordClass;

  final Map<Class<?>, Pickler<?>> delegatePicklers = new HashMap<>();

  public RecordPickler(final Class<R> recordClass) {
    Objects.requireNonNull(recordClass);
    if (!recordClass.isRecord()) {
      throw new IllegalArgumentException("Class " + recordClass.getName() + " is not a record");
    }
    LOGGER.info("Creating RecordPickler for: " + recordClass.getName());
    this.recordClass = recordClass;
    reflection = RecordReflection.analyze(recordClass);
    
    // Note: delegate picklers will be populated by Companion.populateDelegatePicklers()
    // This maintains separation between analysis (here) and construction (in Companion)
  }

  // Package-private constructor for delegation
  RecordPickler(final Class<R> recordClass, RecordReflection<R> reflection) {
    Objects.requireNonNull(recordClass);
    Objects.requireNonNull(reflection);
    if (!recordClass.isRecord()) {
      throw new IllegalArgumentException("Class " + recordClass.getName() + " is not a record");
    }
    this.recordClass = recordClass;
    this.reflection = reflection;
  }

  public String classToInternedName(Class<?> type){
    Objects.requireNonNull(type);
    return reflection.classToInternedName().get(type);
  }

  public Class<?> internedNameToClass(String name){
    Objects.requireNonNull(name);
    return reflection.shortNameToClass().get(name);
  }

  public WriteBuffer wrapForWriting(ByteBuffer buf) {
    return new WriteBufferImpl(buf, this::classToInternedName);
  }

  public ReadBuffer allocateForReading(int size) {
    return new ReadBufferImpl(ByteBuffer.allocate(size), this::internedNameToClass);
  }

  public ReadBuffer wrapForReading(ByteBuffer buf) {
    ReadBufferImpl buffer = new ReadBufferImpl(buf, this::internedNameToClass);
    buffer.parentReflection = this.reflection;
    return buffer;
  }

  public int serialize(WriteBuffer buffer, R object) {
    // Validations
    Objects.requireNonNull(buffer);
    if (buffer.isClosed()) {
      throw new IllegalStateException("WriteBuffer is closed");
    }
    final var buf = ((WriteBufferImpl) buffer);
    final var startPos = buf.position();
    // Ensure java native endian writes
    buf.buffer.order(java.nio.ByteOrder.BIG_ENDIAN);
    this.reflection.serialize(buf, object);
    return buffer.position() - startPos;
  }

  public R deserialize(ByteBuffer buffer) {
    buffer.order(java.nio.ByteOrder.BIG_ENDIAN);
    try {
      return null;//this.reflection.deserialize(buffer);
    } catch (RuntimeException e) {
      throw e;
    } catch (Throwable t) {
      throw new RuntimeException("Failed to deserialize " + recordClass.getName() + " : " + t.getMessage(), t);
    }
  }

  public WriteBuffer allocateForWriting(int totalSize) {
    WriteBufferImpl buffer = new WriteBufferImpl(ByteBuffer.allocate(totalSize), this::classToInternedName);
    buffer.parentReflection = this.reflection;
    return buffer;
  }

  public R deserializeWithMap(ReadBufferImpl buf) throws Throwable {
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
    buffer.reset();
    // Read the interned name
    return reflection.deserialize(buf);
  }

  public int maxSizeOf(R record) {
    return reflection.maxSize(record);
  }
  
  /// Package-private method to get class name mappings for use in SealedPickler
  ClassNameMappings getClassNameMappings() {
    return ClassNameMappings.fromRecordReflection(reflection);
  }
}
