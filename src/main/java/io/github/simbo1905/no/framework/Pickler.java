package io.github.simbo1905.no.framework;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.IntStream;

import static io.github.simbo1905.no.framework.Companion.*;
import static io.github.simbo1905.no.framework.Constants.ARRAY;
import static java.nio.charset.StandardCharsets.UTF_8;

/// No Framework Pickler: A tiny, fast, type-safe, zero-dependency Java serialization library.
/// This interface provides type-safe serialization for records to and from ByteBuffers using reflection-free
/// [Direct Method Handles](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/lang/invoke/MethodHandleInfo.html#directmh).
/// You obtain a pickler for a record type using one of two static method [Pickler#forRecord(java.lang.Class)]
/// or [Pickler#forSealedInterface(java.lang.Class)]. The returned pickler is type-safe and are cached in a concurrent
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
public interface Pickler<T> {
  java.util.logging.Logger LOGGER = java.util.logging.Logger.getLogger(Pickler.class.getName());

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
    /// Use [Pickler#compatibility()] to check the compatibility mode.
    public static final String COMPATIBILITY_SYSTEM_PROPERTY = "no.framework.Pickler.Compatibility";

    /// Given the [Pickler#compatibility()] this method validates the number of components unloaded from the ByteBuffer
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

  /// Obtains the cached a pickler for a record type or creates a new one and adds it into the cache.
  /// This method uses a concurrent map to store the picklers so it is thread-safe.
  /// Throws IllegalArgumentException at runtime if:
  /// - The supplied class is not a record type.
  /// - Any components of the record are not types that are supported by this library.
  /// See [Constants] for details of supported value types.
  static <R extends Record> Pickler<R> forRecord(Class<R> recordClass) {
    return getOrCreate(recordClass, () -> manufactureRecordPickler(recordClass));
  }

  /// Obtains the cached a pickler for a sealed interface that creates picklers for all permitted record types.
  /// It creates a record pickler for each permitted record type and caches them in the returned object.
  /// This method uses a concurrent map to store the top-level picker that is returned so it is thread-safe.
  /// Throws IllegalArgumentException at runtime if:
  /// - The supplied class is not a sealed interface.
  /// - Any permitted subclasses are not record types that are supported by this library.
  /// - Any permitted subclasses of any nested sealed interfaces are not record types that are supported by this library.
  /// See [Constants] for details of supported value types.
  static <S> Pickler<S> forSealedInterface(Class<S> sealedClass) {
    return getOrCreate(sealedClass, () -> manufactureSealedPickler(sealedClass));
  }

  /// Recursively loads the components reachable through record into the buffer. It always writes out all the components.
  /// Older codebase can be set to ignore the extra fields in the buffer if the compatibility mode is set to `FORWARDS`.
  /// @param record The record to serialize
  /// @param buffer The buffer to write into
  void serialize(T record, ByteBuffer buffer);

  /// Recursively unloads components from the buffer and invokes the matching constructor.
  /// By default, the compatibility mode is set to `NONE` and the components encoded into the buffer must exactly match
  /// the current record definition which defines the canonical constructor. This is the default and most secure option.
  /// Older codebase can be set to ignore the extra fields in the buffer if the compatibility mode is set to `FORWARDS`.
  /// Newer codebase set the compatibility mode to `BACKWARDS` and the logic will attempt to use alternative constructors
  /// that match the source code order of the older code version. If the compatibility mode is set to `ALL` then both
  /// `BACKWARDS` and `FORWARDS` compatibility are enabled.
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
  /// [Pickler#deserializeMany(java.lang.Class, java.nio.ByteBuffer)] for deserialization.
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

    @SuppressWarnings("unchecked") Pickler<R> pickler = Pickler.forRecord((Class<R>) array.getClass().getComponentType());
    Arrays.stream(array).forEach(element -> pickler.serialize(element, buffer));
  }

  /// Unloads from the buffer a list of messages that were written using [#serializeMany].
  /// By default, there must be an exact match between the class name of the record and the class name in the buffer.
  /// See [Compatibility] for details of how to change this behaviour to allow for both backwards and forward compatibility.
  static <R extends Record> List<R> deserializeMany(Class<R> componentType, ByteBuffer buffer) {
    buffer.order(java.nio.ByteOrder.BIG_ENDIAN);
    byte marker = buffer.get();
    if (marker != typeMarker(ARRAY)) throw new IllegalArgumentException("Invalid array marker");

    return IntStream.range(0, buffer.getInt())
        .mapToObj(i -> Pickler.forRecord(componentType).deserialize(buffer))
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
                .mapToInt(Pickler.forRecord((Class<R>) arr.getClass().getComponentType())::sizeOf)
                .sum())
        .orElse(1);
  }
}

abstract class SealedPickler<S> implements Pickler<S> {

  abstract Class<? extends S> resolveCachedClassByPickedName(ByteBuffer buffer);
}

abstract class RecordPickler<R extends Record> implements Pickler<R> {

  abstract void serializeWithMap(Map<Class<?>, Integer> classToOffset, Work work, R object);

  abstract R deserializeWithMap(ByteBuffer buffer, Map<Integer, Class<?>> bufferOffset2Class);
}

