package io.github.simbo1905.simple_pickle;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.reflect.Constructor;
import java.lang.reflect.RecordComponent;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

import static io.github.simbo1905.simple_pickle.Pickler.PicklerBase.*;

public interface Pickler2<T> {
  java.util.logging.Logger LOGGER = java.util.logging.Logger.getLogger(Pickler2.class.getName());

  Map<Class<?>, Pickler2<?>> REGISTRY = new ConcurrentHashMap<>();

  // In Pickler2 interface
  @SuppressWarnings("unchecked")
  static <T> Pickler2<T> getOrCreate(Class<T> type, Supplier<Pickler2<T>> supplier) {
    return (Pickler2<T>) REGISTRY.computeIfAbsent(type, k -> supplier.get());
  }

  void serialize(T object, ByteBuffer buffer);

  T deserialize(ByteBuffer buffer);

  int sizeOf(T value);

  static <R extends Record> Pickler2<R> forRecord(Class<R> recordClass) {
    return RecordPickler.create(recordClass);
  }

  static <S> Pickler2<S> forSealedInterface(Class<S> sealedClass) {
    return SealedPickler.create(sealedClass);
  }
}

class SealedPickler<S> implements Pickler2<S> {
  private final Map<Class<? extends S>, Pickler2<? extends S>> subPicklers = new ConcurrentHashMap<>();

  @Override
  public S deserialize(ByteBuffer buffer) {
    // Stub implementation
    return null;
  }

  @Override
  public void serialize(S object, ByteBuffer buffer) {
    // Stub
  }

  @Override
  public int sizeOf(S value) {
    return 0; // Stub
  }

  static <S> Pickler2<S> create(Class<S> sealedClass) {
    return new SealedPickler<>();
  }
}

abstract class RecordPickler<R extends Record> implements Pickler2<R> {

  static <R extends Record> Pickler2<R> create(Class<R> recordClass) {
    return Pickler2.getOrCreate(recordClass, () -> manufactorePickler(recordClass));
  }

  private static <R extends Record> Pickler2<R> manufactorePickler(Class<R> recordClass) {
    MethodHandle[] componentAccessors;
    MethodHandle canonicalConstructorHandle;
    Map<Integer, MethodHandle> fallbackConstructorHandles = new HashMap<>();
    int canonicalParamCount;

    // Get lookup object
    MethodHandles.Lookup lookup = MethodHandles.lookup();

    // First we get the accessor method handles for the record components and add them to the array used
    // that is used by the base class to pull all the components out of the record to load into the byte buffer
    try {
      RecordComponent[] components = recordClass.getRecordComponents();
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
      // Get the record components
      RecordComponent[] components = recordClass.getRecordComponents();
      // Extract component types for the canonical constructor
      Class<?>[] canonicalParamTypes = Arrays.stream(components)
          .map(RecordComponent::getType)
          .toArray(Class<?>[]::new);
      canonicalParamCount = canonicalParamTypes.length;

      // Get all public constructors
      Constructor<?>[] allConstructors = recordClass.getConstructors();

      // Find the canonical constructor and potential fallback constructors
      canonicalConstructorHandle = null;

      for (Constructor<?> constructor : allConstructors) {
        Class<?>[] currentParamTypes = constructor.getParameterTypes();
        int currentParamCount = constructor.getParameterCount();
        MethodHandle handle;

        try {
          handle = lookup.unreflectConstructor(constructor);
        } catch (IllegalAccessException e) {
          LOGGER.warning("Cannot access constructor with " + currentParamCount +
              " parameters for " + recordClass.getName() + ": " + e.getMessage());
          continue;
        }

        if (Arrays.equals(currentParamTypes, canonicalParamTypes)) {
          // Found the canonical constructor
          canonicalConstructorHandle = handle;
        } else {
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
      }

      // If we didn't find the canonical constructor, try to find it directly
      if (canonicalConstructorHandle == null) {
        try {
          // Create method type for the canonical constructor
          MethodType constructorType = MethodType.methodType(void.class, canonicalParamTypes);
          canonicalConstructorHandle = lookup.findConstructor(recordClass, constructorType);
        } catch (NoSuchMethodException | IllegalAccessException e) {
          final var msg = "Failed to access canonical constructor for record '" +
              recordClass.getName() + "' due to " + e.getClass().getSimpleName();
          LOGGER.severe(() -> msg);
          throw new IllegalArgumentException(msg, e);
        }
      }
    } catch (Exception e) {
      final var msg = "Failed to access constructors for record '" +
          recordClass.getName() + "' due to " + e.getClass().getSimpleName() + " " + e.getMessage();
      LOGGER.severe(() -> msg);
      throw new IllegalArgumentException(msg, e);
    }

    // Capture these values for use in the anonymous class
    final MethodHandle finalCanonicalConstructorHandle = canonicalConstructorHandle;
    final int finalCanonicalParamCount = canonicalParamCount;
    final String recordClassName = recordClass.getName();
    final Map<Integer, MethodHandle> finalFallbackConstructorHandles =
        Collections.unmodifiableMap(fallbackConstructorHandles);

    return new RecordPickler<>() {
      @Override
      Object[] components(R record) {
        Object[] result = new Object[componentAccessors.length];
        Arrays.setAll(result, i -> {
          try {
            return componentAccessors[i].invokeWithArguments(record);
          } catch (Throwable e) {
            final var msg = "Failed to access component: " + i +
                " in record class '" + recordClassName + "' : " + e.getMessage();
            LOGGER.severe(() -> msg);
            throw new IllegalArgumentException(msg, e);
          }
        });
        return result;
      }

      @SuppressWarnings("unchecked")
      @Override
      R staticCreateFromComponents(Object[] components) {
        try {
          // Get the number of components from the serialized data
          int numComponents = components.length;
          MethodHandle constructorToUse;

          if (numComponents == finalCanonicalParamCount) {
            // Number of components matches the canonical constructor - use it directly
            constructorToUse = finalCanonicalConstructorHandle;
            LOGGER.finest(() -> "Using canonical constructor for " + recordClassName +
                " with " + numComponents + " components");
          } else {
            // Number of components differs, look for a fallback constructor
            constructorToUse = finalFallbackConstructorHandles.get(numComponents);
            if (constructorToUse == null) {
              final var msg = "Schema evolution error: Cannot deserialize data for " +
                  recordClassName + ". Found " + numComponents +
                  " components, but no matching constructor (canonical or fallback) exists.";
              LOGGER.severe(() -> msg);
              // No fallback constructor matches the number of components found
              throw new IllegalArgumentException(msg);
            }
            LOGGER.finest(() -> "Using fallback constructor for " + recordClassName +
                " with " + numComponents + " components");
          }

          // Invoke the selected constructor
          return (R) constructorToUse.invokeWithArguments(components);
        } catch (Throwable e) {
          final var msg = "Failed to create instance of " + recordClassName +
              " with " + components.length + " components: " + e.getMessage();
          LOGGER.severe(() -> msg);
          throw new IllegalArgumentException(msg, e);
        }
      }

      @Override
      public void serialize(R object, ByteBuffer buffer) {
        final var components = components(object);
        // Write the number of components as an unsigned byte (max 255)
        writeUnsignedByte(buffer, (short) components.length);
        Arrays.stream(components).forEach(c -> write(new HashMap<>(), buffer, c));
      }

      @Override
      public R deserialize(ByteBuffer buffer) {
        // Read the number of components as an unsigned byte
        final short length = readUnsignedByte(buffer);
        final var components = new Object[length];
        Arrays.setAll(components, ignored -> deserializeValue(new HashMap<>(), buffer));
        return this.staticCreateFromComponents(components);
      }

      @Override
      public int sizeOf(R object) {
        final var components = components(object);
        int size = 1; // Start with 1 byte for the type of the component
        for (Object c : components) {
          size += staticSizeOf(c, new HashSet<>());
          int finalSize = size;
          LOGGER.finer(() -> "Size of " +
              Optional.ofNullable(c).map(c2 -> c2.getClass().getSimpleName()).orElse("null")
              + " '" + c + "' is " + finalSize);
        }
        return size;
      }
    };
  }

  abstract Object[] components(R record);

  @SuppressWarnings("unchecked")
  abstract R staticCreateFromComponents(Object[] components);
}
