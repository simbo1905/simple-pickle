package io.github.simbo1905.no.framework;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentHashMap;

public interface Pickler<T> {
  java.util.logging.Logger LOGGER = java.util.logging.Logger.getLogger(Pickler.class.getName());

  /// Recursively loads the components reachable through record into the buffer. It always writes out all the components.
  ///
  /// @param buffer The buffer to write into
  /// @param record The record to serialize
  void serialize(ByteBuffer buffer, T record);

  /// Recursively unloads components from the buffer and invokes a constructor following compatibility rules.
  /// @param buffer The buffer to read from
  /// @return The deserialized record
  T deserialize(ByteBuffer buffer);

  ConcurrentHashMap<Class<?>, Pickler<?>> REGISTRY = new ConcurrentHashMap<>();

  // In Pickler interface
  @SuppressWarnings("unchecked")
  static <T> Pickler<T> getOrCreate(Class<T> type) {
    return (Pickler<T>) REGISTRY.computeIfAbsent(type, Pickler::manufactureRecordPickler);
  }

  static <R extends Record> Pickler<R> manufactureRecordPickler(Class<?> recordClass) {
    assert 0 < recordClass.getRecordComponents().length;
    return new RecordPickler<>() {
    };
  }

  static <S> Pickler<S> manufactureSealedPickler(Class<S> sealedClass) {
    return new SealedPickler<>() {
    };
  }

  class SealedPickler<S> implements Pickler<S> {


    @Override
    public S deserialize(ByteBuffer buffer) {
      final var length = buffer.getInt();
      byte[] bytes = new byte[length];
      buffer.get(bytes);
      try (ByteArrayInputStream bin = new ByteArrayInputStream(bytes);
           ObjectInputStream in = new ObjectInputStream(bin)) {
        //noinspection unchecked
        return (S) in.readObject();
      } catch (IOException | ClassNotFoundException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public void serialize(ByteBuffer buffer, S animal) {
      manufactureRecordPickler(animal.getClass()).serialize(buffer, (Record) animal);
    }
  }

  class RecordPickler<R extends Record> implements Pickler<R> {

    @Override
    public void serialize(ByteBuffer buffer, R object) {
      assert 0 < object.getClass().getRecordComponents().length;
      try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
           ObjectOutputStream out = new ObjectOutputStream(baos)) {
        out.writeObject(object);
        out.flush();
        byte[] bytes = baos.toByteArray();
        buffer.putInt(bytes.length);
        buffer.put(bytes);
      } catch (Exception e) {
        throw new RuntimeException("Failed to serialize record", e);
      }
    }

    @Override
    public R deserialize(ByteBuffer buffer) {
      final var length = buffer.getInt();
      byte[] bytes = new byte[length];
      buffer.get(bytes);
      try (ByteArrayInputStream bin = new ByteArrayInputStream(bytes);
           ObjectInputStream in = new ObjectInputStream(bin)) {
        //noinspection unchecked
        return (R) in.readObject();
      } catch (IOException | ClassNotFoundException e) {
        throw new RuntimeException(e);
      }
    }
  }
}

class Companion {

}
