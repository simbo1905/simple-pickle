package io.github.simbo1905.no.framework;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Constructor;
import java.lang.reflect.RecordComponent;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;

import static io.github.simbo1905.no.framework.Companion.nameToBasicClass;
import static io.github.simbo1905.no.framework.Constants.INTERNED_NAME;

final class RecordPickler<R extends Record> implements Pickler<R> {
  final MethodHandle[] componentAccessors;
  final Class<R> recordClass;
  final int componentCount;
  final MethodHandle canonicalConstructorHandle;
  final Map<Integer, MethodHandle> fallbackConstructorHandles = new HashMap<>();
  final InternedName internedName;

  RecordPickler(final Class<R> recordClass, InternedName internedName) {
    this.internedName = internedName;
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
      // Filter out the canonical constructor and keep the others as fallbacks
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

  void serializeWithMap(PackedBuffer buffer, R object) {
    Object[] components = new Object[componentAccessors.length];
    Arrays.setAll(components, i -> {
      try {
        return componentAccessors[i].invokeWithArguments(object);
      } catch (Throwable e) {
        final var msg = "Failed to access component: " + i +
            " in record class '" + recordClass.getName() + "' : " + e.getMessage();
        LOGGER.severe(() -> msg);
        throw new IllegalArgumentException(msg, e);
      }
    });
    // Write the number of components as an unsigned byte (max 255)
    LOGGER.finer(() -> "serializeWithMap Writing component length length=" + components.length + " position=" + buffer.position());
    ZigZagEncoding.putInt(buffer.buffer, components.length);
    Arrays.stream(components).forEach(c -> {
      buffer.writeComponent(buffer, c);
    });
  }

  @Override
  public void serialize(PackedBuffer buf, R object) {
    // Null checks
    Objects.requireNonNull(buf);
    Objects.requireNonNull(object);
    // Use java native endian for float and double writes
    buf.buffer.order(java.nio.ByteOrder.BIG_ENDIAN);
    // The following `assert` requires jvm flag `-ea` to run. Here we check for a common problem where Java erasure
    // can have you accidentally create arrays of records from collections that are the common supertype of all your records.
    assert 0 == object.getClass().getRecordComponents().length : object.getClass().getName() + " has no components. Built-in collections conversion to arrays may cause this problem.";
    // If we are being asked to write out our record class name by a sealed pickler then we so now
    Optional.ofNullable(internedName).ifPresent(name -> {
      buf.offsetMap.put(internedName, new InternedPosition(buf.buffer.position()));
      Companion.write(buf.buffer, internedName);
    });
    // Write the all the components
    serializeWithMap(buf, object);
  }

  @Override
  public R deserialize(ByteBuffer buffer) {
    buffer.order(java.nio.ByteOrder.BIG_ENDIAN);
    try {
      Optional.ofNullable(internedName).ifPresent(name -> {
        final byte marker = buffer.get();
        if (marker != INTERNED_NAME.marker()) {
          throw new IllegalStateException("Expected marker " + INTERNED_NAME.marker() + " but got " + marker);
        }
        // Read the interned name
        final int length = ZigZagEncoding.getInt(buffer);
        byte[] bytes = new byte[length];
        buffer.get(bytes);
        final var internedName = new InternedName(new String(bytes, StandardCharsets.UTF_8));
        if (!internedName.equals(this.internedName)) {
          throw new IllegalStateException("Interned name mismatch: expected " + this.internedName + " but got " + internedName);
        }
        LOGGER.finer(() -> "deserializeWithMap reading interned name " + internedName.name() + " position=" + buffer.position());
      });
      return deserializeWithMap(new HashMap<>(nameToBasicClass), buffer);
    } catch (RuntimeException e) {
      throw e;
    } catch (Throwable t) {
      throw new RuntimeException("Failed to deserialize " + recordClass.getName() + " : " + t.getMessage(), t);
    }
  }

  @Override
  public int sizeOf(Object record) {
    throw new AssertionError("not implemented");
  }

  R deserializeWithMap(final Map<String, Class<?>> nameToRecordClass, ByteBuffer buffer) throws Throwable {
    // Read the number of components as an unsigned byte
    LOGGER.finer(() -> "deserializeWithMap reading component length position=" + buffer.position());
    final int length = ZigZagEncoding.getInt(buffer);
    final Object[] components = new Object[length];
    Arrays.setAll(components, i -> {
      try {
        // Read the component
        return Companion.read(nameToRecordClass, buffer);
      } catch (Throwable e) {
        final var msg = "Failed to access component: " + i +
            " in record class '" + recordClass.getName() + "' : " + e.getMessage();
        LOGGER.severe(() -> msg);
        throw new IllegalArgumentException(msg, e);
      }
    });
    //noinspection unchecked
    return (R) canonicalConstructorHandle.invokeWithArguments(components);
  }
}
