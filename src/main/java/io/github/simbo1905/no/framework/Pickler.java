package io.github.simbo1905.no.framework;

import java.io.*;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Constructor;
import java.lang.reflect.RecordComponent;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

import static io.github.simbo1905.no.framework.Companion.manufactureRecordPickler;

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

  static <R extends Record> Pickler<R> forRecord(Class<R> recordClass) {
    return Companion.getOrCreate(recordClass, () -> manufactureRecordPickler(recordClass));
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
      //manufactureRecordPickler(animal.getClass()).serialize(buffer, (Record) animal);
    }
  }

  final class RecordPickler<R extends Record> implements Pickler<R> {
    final MethodHandle[] componentAccessors;
    final Class<R> recordClass;
    final int componentCount;
    final MethodHandle canonicalConstructorHandle;
    final Map<Integer, MethodHandle> fallbackConstructorHandles = new HashMap<>();

    RecordPickler(final Class<R> recordClass) {
      this.recordClass = recordClass;
      final RecordComponent[] components = recordClass.getRecordComponents();
      componentCount = components.length;
      final MethodHandles.Lookup lookup = MethodHandles.lookup();
      try {
        // Get parameter types for the canonical constructor
        Class<?>[] parameterTypes = Arrays.stream(components)
            .map(RecordComponent::getType)
            .toArray(Class<?>[]::new);

        // Get the canonical constructor
        Constructor<?> constructorHandle = recordClass.getDeclaredConstructor(parameterTypes);
        canonicalConstructorHandle = lookup.unreflectConstructor(constructorHandle);

        componentAccessors = new MethodHandle[components.length];
        Arrays.setAll(componentAccessors, i -> {
          try {
            return lookup.unreflect(components[i].getAccessor());
          } catch (IllegalAccessException e) {
            final var msg = "Failed to access component accessor for " + components[i].getName() +
                " in record class " + recordClass.getName() + ": " + e.getClass().getSimpleName();
            LOGGER.severe(() -> msg);
            throw new IllegalArgumentException(msg, e);
          }
        });
      } catch (Exception e) {
        Throwable inner = e;
        while (inner.getCause() != null) {
          inner = inner.getCause();
        }
        final var msg = "Failed to access record components for class '" +
            recordClass.getName() + "' due to " + inner.getClass().getSimpleName() + " " + inner.getMessage();
        LOGGER.severe(() -> msg);
        throw new IllegalArgumentException(msg, inner);
      }

      // Get the canonical constructor and any fallback constructors for schema evolution
      try {
        // Get all public constructors
        final Constructor<?>[] allConstructors = recordClass.getConstructors();

        for (Constructor<?> constructor : allConstructors) {
          int currentParamCount = constructor.getParameterCount();
          MethodHandle handle;

          try {
            handle = lookup.unreflectConstructor(constructor);
          } catch (IllegalAccessException e) {
            LOGGER.warning("Cannot access constructor with " + currentParamCount +
                " parameters for " + recordClass.getName() + ": " + e.getMessage());
            continue;
          }

          // This is a potential fallback constructor for schema evolution
          if (fallbackConstructorHandles.containsKey(currentParamCount)) {
            LOGGER.warning("Multiple fallback constructors with " + currentParamCount +
                " parameters found for " + recordClass.getName() +
                ". Using the first one encountered.");
            // We keep the first one we found
          } else {
            fallbackConstructorHandles.put(currentParamCount, handle);
            LOGGER.fine("Found fallback constructor with " + currentParamCount +
                " parameters for " + recordClass.getName());
          }
        }
      } catch (Exception e) {
        final var msg = "Failed to access constructors for record '" +
            recordClass.getName() + "' due to " + e.getClass().getSimpleName() + " " + e.getMessage();
        LOGGER.severe(() -> msg);
        throw new IllegalArgumentException(msg, e);
      }
    }

    @Override
    public void serialize(ByteBuffer buffer, R object) {
      assert 0 < object.getClass().getRecordComponents().length : object.getClass().getName() + " has no components. Built-in collections conversion to arrays may cause this problem.";
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

  static ConcurrentHashMap<Class<?>, Pickler<?>> REGISTRY = new ConcurrentHashMap<>();

  @SuppressWarnings("unchecked")
  static <T> Pickler<T> getOrCreate(Class<T> type, Supplier<Pickler<T>> supplier) {
    return (Pickler<T>) REGISTRY.computeIfAbsent(type, k -> supplier.get());
  }

  static <R extends Record> Pickler<R> manufactureRecordPickler(Class<R> recordClass) {
    return new Pickler.RecordPickler<>(recordClass);
  }
}
