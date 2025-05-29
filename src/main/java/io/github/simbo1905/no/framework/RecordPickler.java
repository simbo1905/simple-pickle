package io.github.simbo1905.no.framework;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Objects;

final class RecordPickler<R extends Record> implements Pickler<R> {

  final RecordReflection<R> reflection;
  private final Class<R> recordClass;

  public RecordPickler(final Class<R> recordClass) {
    Objects.requireNonNull(recordClass);
    if (!recordClass.isRecord()) {
      throw new IllegalArgumentException("Class " + recordClass.getName() + " is not a record");
    }
    this.recordClass = recordClass;
    reflection = RecordReflection.analyze(recordClass);
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

  @Override
  public WriteBuffer wrap(ByteBuffer buf) {
    return new WriteBufferImpl(buf, this::classToInternedName);
  }

  public ReadBuffer wrapForReading(ByteBuffer buf) {
    return new ReadBufferImpl(buf, this::internedNameToClass);
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
    buf.buffer.order(java.nio.ByteOrder.BIG_ENDIAN);
    buf.currentRecordClass = recordClass;
    try {
      return this.reflection.deserialize(buf);
    } catch (RuntimeException e) {
      throw e;
    } catch (Throwable t) {
      throw new RuntimeException("Failed to deserialize " + recordClass.getName() + " : " + t.getMessage(), t);
    }
  }

  @Override
  public WriteBuffer allocateSufficient(R record) {
    Objects.requireNonNull(record, "Record must not be null");
    int maxSize = reflection.maxSize(record);
    WriteBufferImpl buffer = new WriteBufferImpl(ByteBuffer.allocate(maxSize), this::classToInternedName);
    buffer.parentReflection = this.reflection;
    return buffer;
  }

  @Override
  public WriteBuffer allocateSufficient(R[] record) {
    int totalSize = Arrays.stream(record)
            .mapToInt(reflection::maxSize)
            .sum();
    WriteBufferImpl buffer = new WriteBufferImpl(ByteBuffer.allocate(totalSize), this::classToInternedName);
    buffer.parentReflection = this.reflection;
    return buffer;
  }

  @Override
  public WriteBuffer allocate(int totalSize) {
    WriteBufferImpl buffer = new WriteBufferImpl(ByteBuffer.allocate(totalSize), this::classToInternedName);
    buffer.parentReflection = this.reflection;
    return buffer;
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
    return reflection.deserialize(buf);
  }
}
