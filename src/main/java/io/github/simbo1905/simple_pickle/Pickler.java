// SPDX-FileCopyrightText: 2025 Simon Massey
// SPDX-License-Identifier: Apache-2.0
package io.github.simbo1905.simple_pickle;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.reflect.RecordComponent;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;

/// Interface for serializing and deserializing objects.
///
/// @param <T> The type of object to be pickled
/// TODO manufacture an estimate size of the object to be pickled
public interface Pickler<T> {
  /// Registry to store Picklers by class to avoid redundant creation and infinite recursion
  ConcurrentHashMap<Class<?>, Pickler<?>> PICKLER_REGISTRY = new ConcurrentHashMap<>();

  /// Serializes an object into a byte buffer.
  ///
  /// @param object The object to serialize
  /// @param buffer The buffer to write to
  void serialize(T object, ByteBuffer buffer);

  /// Deserializes an object from a byte buffer.
  ///
  /// @param buffer The buffer to read from
  /// @return The deserialized object
  T deserialize(ByteBuffer buffer);

  /// Get a Pickler for a record class, creating one if it doesn't exist in the registry
  @SuppressWarnings("unchecked")
  static <R extends Record> Pickler<R> picklerForRecord(Class<R> recordClass) {
    // Check if we already have a Pickler for this record class
    return (Pickler<R>) PICKLER_REGISTRY.computeIfAbsent(recordClass, clazz -> {
      return createPicklerForRecord((Class<R>) clazz);
    });
  }

  /// Internal method to create a new Pickler for a record class
  private static <R extends Record> Pickler<R> createPicklerForRecord(Class<R> recordClass) {
    return new PicklerBase<>() {
      @Override
      Object[] staticGetComponents(R record) {
        Object[] result = new Object[componentAccessors.length];
        Arrays.setAll(result, i -> {
          try {
            return componentAccessors[i].invokeWithArguments(record);
          } catch (Throwable e) {
            throw new RuntimeException("Failed to access component: " + i, e);
          }
        });
        return result;
      }

      @Override
      R staticCreateFromComponents(Object[] components) {
        try {
          //noinspection unchecked
          return (R) super.constructorHandle.invokeWithArguments(components);
        } catch (Throwable e) {
          throw new RuntimeException(e);
        }
      }

      // object initialization block
      {
        // first we get the accessor method handles for the record components used to serialize it
        try {
          MethodHandles.Lookup lookup = MethodHandles.lookup();

          RecordComponent[] components = recordClass.getRecordComponents();
          componentAccessors = new MethodHandle[components.length];
          Arrays.setAll(componentAccessors, i -> {
            try {
              return lookup.unreflect(components[i].getAccessor());
            } catch (IllegalAccessException e) {
              throw new RuntimeException("Failed to access component: " + components[i].getName(), e);
            }
          });
        } catch (Exception e) {
          throw new RuntimeException("Failed to create accessor method handles for record components", e);
        }
        // Then we get the constructor method handle for the record
        try {
          // Get the record components
          RecordComponent[] components = recordClass.getRecordComponents();
          // Extract component types for the constructor
          Class<?>[] paramTypes = Arrays.stream(components)
              .map(RecordComponent::getType)
              .toArray(Class<?>[]::new);
          // Create method type for the canonical constructor
          MethodType constructorType = MethodType.methodType(void.class, paramTypes);

          // Get lookup object
          MethodHandles.Lookup lookup = MethodHandles.lookup();

          constructorHandle = lookup.findConstructor(recordClass, constructorType);

        } catch (NoSuchMethodException | IllegalAccessException e) {
          throw new RuntimeException("Failed to access constructor for record: " + recordClass.getName(), e);
        }
      }
    };
  }
}
