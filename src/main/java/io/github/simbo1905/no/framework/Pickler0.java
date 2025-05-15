package io.github.simbo1905.no.framework;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Constructor;
import java.lang.reflect.RecordComponent;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.stream.IntStream;

import static io.github.simbo1905.no.framework.Companion0.*;
import static io.github.simbo1905.no.framework.Constants0.ARRAY;
import static java.nio.charset.StandardCharsets.UTF_8;

/// No Framework Pickler: A tiny, fast, type-safe, zero-dependency Java serialization library.
/// This interface provides type-safe serialization for records to and from ByteBuffers using reflection-free
/// [Direct Method Handles](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/lang/invoke/MethodHandleInfo.html#directmh).
/// You obtain a pickler for a record type using one of two static method [Pickler0#forRecord(java.lang.Class)]
/// or [Pickler0#forSealedInterface(java.lang.Class)]. The returned pickler is type-safe and are cached in a concurrent
/// static map. The picklers can then be used to serialize and deserialize the record type to and from a ByteBuffer:
///
///  - `void serialize(T record, ByteBuffer buffer)` recursively loads the components reachable through record T to the buffer.
///  - `T deserialize(ByteBuffer buffer)` recursively unloads components from the buffer and invokes the matching constructor.
///  - `int sizeOf(T record)` recursively sums the encoded byte size of this record type.
///  - `void serializeMany(R[] array, ByteBuffer buffer)` serializes an array of objects.
///  - `List<R> deserializeMany(Class<R> componentType, ByteBuffer buffer)` deserializes an array of objects.
///  - `int sizeOfMany(R[] array)` recursively sums the encoded byte size of many records.
///
/// Key features:
/// - Zero dependencies, single Java file (~1,100 LOC), tiny jar (~33k)
/// - Works with nested sealed interfaces of permitted record types
/// - Supports primitive types, String, Optional, Record, Map, List, Enum, Arrays
/// - Fast performance by caching MethodHandles instead of using reflection
/// - Secure by default with strict validation
/// - Immutable collections in deserialized results
/// - Binary backwards and forwards compatibility through alternative constructors
///
/// See [Pickler#forRecord(java.lang.Class)
/// See the [Compatibility] enum which allows for backwards and forwards compatibility between pickers.
///
/// Logging is done using the standard Java logging framework java.util.logger known as "jul" logging.
/// There are ways to bridge that to other logging frameworks like SLF4J or Log4j2.
/// Errors are logged at the SEVERE level. If backwards compatibility is enabled, warnings are logged at the WARNING level.
///
/// @param <T> The type to be serialized/deserialized which may be a Record type or a sealed interface that
/// may contain nested sealed interfaces where the only concrete types must be Records containing simple types
/// or nested Records containing simple types.
public interface Pickler0<T> {
  java.util.logging.Logger LOGGER = java.util.logging.Logger.getLogger(Pickler0.class.getName());

  /// Obtains the cached a pickler for a record type or creates a new one and adds it into the cache.
  /// This method uses a concurrent map to store the picklers so it is thread-safe.
  /// Throws IllegalArgumentException at runtime if:
  /// - The supplied class is not a record type.
  /// - Any components of the record are not types that are supported by this library.
  /// See [Constants0] for details of supported value types.
  static <R extends Record> Pickler0<R> forRecord(Class<R> recordClass) {
    return getOrCreate(recordClass, () -> manufactureRecordPickler(recordClass));
  }

  /// Obtains the cached a pickler for a sealed interface that creates picklers for all permitted record types.
  /// It creates a record pickler for each permitted record type and caches them in the returned object.
  /// This method uses a concurrent map to store the top-level picker that is returned so it is thread-safe.
  /// Throws IllegalArgumentException at runtime if:
  /// - The supplied class is not a sealed interface.
  /// - Any permitted subclasses are not record types that are supported by this library.
  /// - Any permitted subclasses of any nested sealed interfaces are not record types that are supported by this library.
  /// See [Constants0] for details of supported value types.
  static <S> Pickler0<S> forSealedInterface(Class<S> sealedClass) {
    return getOrCreate(sealedClass, () -> manufactureSealedPickler(sealedClass));
  }

  /// Recursively loads the components reachable through record into the buffer. It always writes out all the components.
  ///
  /// @param buffer The buffer to write into
  /// @param record The record to serialize
  void serialize(ByteBuffer buffer, T record);

  /// Recursively unloads components from the buffer and invokes a constructor following compatibility rules.
  /// @param buffer The buffer to read from
  /// @return The deserialized record
  T deserialize(ByteBuffer buffer);

  /// Recursively sums the encoded byte size of this record type. Note this may be quite a lot of work if the record
  /// given is the root node in a massive nested tree of a hierarchy of records. If you know your records are always
  /// small then it **may** be better to recycle buffers that are allocated to be  larger than your expected max size.
  /// @param record The record to measure
  /// @return The size in bytes
  int sizeOf(T record);

  /// A convenient helper to serializes an array of records. Due to Java's runtime type erasure you must use
  /// explicitly declared arrays and not have any compiler warnings about possible misalignment of types. Use
  /// [Pickler0#deserializeMany(java.lang.Class, java.nio.ByteBuffer)] for deserialization.
  ///
  /// WARNING: Do not attempt to use helper methods on Java collections into convert Collections into arrays!
  ///
  /// The boring safe way to make an array from a list of records is to do an explicit shallow copy into an explicitly
  /// instantiated array:
  ///
  /// ```
  /// People[] people = new People[peopleList.size()]
  /// Arrays.setAll(people, i -> peopleList.get(i));
  ///```
  ///
  /// The shallow copy is the price to pay for a type safe way to serialization many things.
  /// See the `README.md` for a discussion on how to void the shallow copy.
  ///
  /// @param array The array to serialize
  /// @param buffer The buffer to write into
  static <R extends Record> void serializeMany(R[] array, ByteBuffer buffer) {
    buffer.order(java.nio.ByteOrder.BIG_ENDIAN);
    buffer.put(typeMarker(ARRAY));
    buffer.putInt(array.length);

    @SuppressWarnings("unchecked") Pickler0<R> pickler = Pickler0.forRecord((Class<R>) array.getClass().getComponentType());
    Arrays.stream(array).forEach(element -> pickler.serialize(buffer, element));
  }

  /// Unloads from the buffer a list of messages that were written using [#serializeMany].
  /// By default, there must be an exact match between the class name of the record and the class name in the buffer.
  /// See [Compatibility] for details of how to change this behaviour to allow for both backwards and forward compatibility.
  static <R extends Record> List<R> deserializeMany(Class<R> componentType, ByteBuffer buffer) {
    buffer.order(java.nio.ByteOrder.BIG_ENDIAN);
    byte marker = buffer.get();
    if (marker != typeMarker(ARRAY)) throw new IllegalArgumentException("Invalid array marker");

    return IntStream.range(0, buffer.getInt())
        .mapToObj(i -> Pickler0.forRecord(componentType).deserialize(buffer))
        .toList();
  }

  /// Recursively sums the encoded byte size of many records.
  /// @param array The array of records to measure
  /// @return The total size in bytes
  static <R extends Record> int sizeOfMany(R[] array) {
    //noinspection unchecked
    return Optional.ofNullable(array)
        .map(arr -> 1 + 4 // 4 bytes for the length prefix (int)
            + arr.getClass().getComponentType().getName().getBytes(UTF_8).length + 4 +
            Arrays.stream(arr)
                .mapToInt(Pickler0.forRecord((Class<R>) arr.getClass().getComponentType())::sizeOf)
                .sum())
        .orElse(1);
  }


  void serialize(CompactedBuffer serializationSession, T testRecord);

  /// Controls how records handle schema evolution during deserialization.
  /// The system property [Compatibility#COMPATIBILITY_SYSTEM_PROPERTY] may be set to the following behaviours.
  ///
  /// `NONE`: No schema evolution support. The serialized record must exactly match
  ///       the current record definition. This is the default and most secure option.
  ///
  /// `BACKWARDS`: The current code can read data written by older versions.
  ///            Allows fewer fields in the buffer than in the current record definition.
  ///            Requires fallback constructors in the current code that match source code
  ///            order of the older code version.
  ///
  /// `FORWARDS`: The current code can read data written by newer versions.
  ///           Allows more fields in the buffer than in the current record definition.
  ///           Extra fields in the buffer will be ignored.
  ///
  /// `ALL`: Enables both `BACKWARDS` and `FORWARDS` compatibility.
  enum Compatibility {
    NONE,
    BACKWARDS,
    FORWARDS,
    ALL;

    /// The system property name used to set the compatibility mode when a given pickler is created.
    /// Use [Pickler0#compatibility()] to check the compatibility mode.
    public static final String COMPATIBILITY_SYSTEM_PROPERTY = "no.framework.Pickler.Compatibility";

    /// Given the [Pickler0#compatibility()] this method validates the number of components unloaded from the ByteBuffer
    /// against the count of components of the canonical constructor. The default compatibility mode is `NONE` so the
    /// lengths must match.
    /// It throws an IllegalArgumentException if the count of components in the buffer is not compatible.
    static void validate(final Compatibility compatibility, final String recordClassName, final int componentCount, final int bufferLength) {
      if (compatibility == Compatibility.ALL) {
        return;
      }
      if (compatibility == Compatibility.NONE && bufferLength != componentCount) {
        throw new IllegalArgumentException("Failed to create instance for class %s with Compatibility.NONE yet buffer length %s != component count %s"
            .formatted(recordClassName, bufferLength, componentCount));
      } else if (compatibility == Compatibility.BACKWARDS && bufferLength > componentCount) {
        throw new IllegalArgumentException("Failed to create instance for class %s with Compatibility.BACKWARDS and count of components %s > buffer size %s"
            .formatted(recordClassName, bufferLength, componentCount));
      } else if (compatibility == Compatibility.FORWARDS && bufferLength < componentCount) {
        throw new IllegalArgumentException("Failed to create instance for class %s with Compatibility.FORWARDS and count of components %s < buffer size %s"
            .formatted(recordClassName, bufferLength, componentCount));
      }
    }
  }


  /// Returns the compatibility mode for this pickler. See [Compatibility] for details of how to set via a system property.
  Compatibility compatibility();


}

abstract class SealedPickler0<S> implements Pickler0<S> {

  abstract Class<? extends S> resolveCachedClassByPickedName(ByteBuffer buffer);
}

abstract class RecordPickler0<R extends Record> implements Pickler0<R> {
  final MethodHandle[] componentAccessors;
  final Class<R> recordClass;
  final int componentCount;
  final MethodHandle canonicalConstructorHandle;
  final Map<Integer, MethodHandle> fallbackConstructorHandles = new HashMap<>();
  final Compatibility compatibility = Pickler0.Compatibility.valueOf(System.getProperty(
      Pickler0.Compatibility.COMPATIBILITY_SYSTEM_PROPERTY,
      Pickler0.Compatibility.NONE.name()));

  RecordPickler0(final Class<R> recordClass) {
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

    final Pickler0.Compatibility compatibility = Pickler0.Compatibility.valueOf(
        System.getProperty(Pickler0.Compatibility.COMPATIBILITY_SYSTEM_PROPERTY, "NONE"));

    final String recordClassName = recordClass.getName();
    if (compatibility != Pickler0.Compatibility.NONE) {
      // We are secure by default this is opt-in and should not be left on forever so best to nag
      LOGGER.warning(() -> "Pickler for " + recordClassName + " has Compatibility set to " + compatibility.name());
    }

    // we are security by default so if we are set to strict mode do not allow fallback constructors
    final Map<Integer, MethodHandle> finalFallbackConstructorHandles =
        (Pickler0.Compatibility.BACKWARDS == compatibility || Pickler0.Compatibility.ALL == compatibility) ?
            Collections.unmodifiableMap(fallbackConstructorHandles) : Collections.emptyMap();
  }

  Object[] components(R record) {
    Object[] result = new Object[componentAccessors.length];
    Arrays.setAll(result, i -> {
      try {
        return componentAccessors[i].invokeWithArguments(record);
      } catch (Throwable e) {
        final var msg = "Failed to access component: " + i +
            " in record class '" + recordClass.getName() + "' : " + e.getMessage();
        LOGGER.severe(() -> msg);
        throw new IllegalArgumentException(msg, e);
      }
    });
    return result;
  }

  void serializeWithMap(CompactedBuffer buffer, R object) {
    final var components = components(object);
    // Write the number of components as an unsigned byte (max 255)
    LOGGER.finer(() -> "serializeWithMap Writing component length length=" + components.length + " position=" + buffer.position());
    buffer.write(components.length);
    Arrays.stream(components).forEach(c -> {
      buffer.writeComponent(c);
    });
  }

  R deserializeWithMap(ByteBuffer buffer, Map<Integer, Class<?>> bufferOffset2Class) {
    // Read the number of components as an unsigned byte
    LOGGER.finer(() -> "deserializeWithMap reading component length position=" + buffer.position());
    final int length = buffer.getInt();
    //Compatibility.validate(compatibility, recordClass.getName(), componentCount, length);
    // This may unload from the stream things that we will ignore
    final Object[] components = new Object[length];
//    Arrays.setAll(components, ignored -> WriteOperations.deserializeValue(bufferOffset2Class, buffer));
//    if (componentCount < length && (Compatibility.FORWARDS == compatibility || Compatibility.ALL == compatibility)) {
//      return this.staticCreateFromComponents(Arrays.copyOfRange(components, 0, componentCount));
//    }
    return this.staticCreateFromComponents(components);
  }

  @SuppressWarnings("unchecked")
  private R staticCreateFromComponents(Object[] components) {
    try {
      // Get the number of components from the serialized data
      int numComponents = components.length;
      MethodHandle constructorToUse;

      if (numComponents == componentCount) {
        // Number of components matches the canonical constructor - use it directly
        constructorToUse = canonicalConstructorHandle;
      } else {
        // Number of components differs, look for a fallback constructor
        constructorToUse = fallbackConstructorHandles.get(numComponents);
        if (constructorToUse == null) {
          final var msg = "Schema evolution error: Cannot deserialize data for " +
              this.recordClass.getName() + ". Found " + numComponents +
              " components, but no matching constructor (canonical or fallback) exists.";
          LOGGER.severe(() -> msg);
          // No fallback constructor matches the number of components found
          throw new IllegalArgumentException(msg);
        }
      }

      // Invoke the selected constructor
      return (R) constructorToUse.invokeWithArguments(components);
    } catch (Throwable e) {
      final var msg = "Failed to create instance of " + this.recordClass.getName() +
          " with " + components.length + " components: " + e.getMessage();
      LOGGER.severe(() -> msg);
      throw new IllegalArgumentException(msg, e);
    }
  }

  public record InternedName(String name) {
  }

  public record InternedOffset(int offset) {
  }

  public record InternedPosition(int position) {
  }
}
