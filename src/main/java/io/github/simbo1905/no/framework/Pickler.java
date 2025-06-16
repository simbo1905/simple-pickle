// SPDX-FileCopyrightText: 2025 Simon Massey
// SPDX-License-Identifier: MIT

package io.github.simbo1905.no.framework;

import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.logging.Logger;

/// Main interface for the No Framework Pickler serialization library.
/// Provides type-safe, reflection-free serialization for records and sealed interfaces.
public sealed interface Pickler<T> permits PicklerImpl, PicklerUsingAst {

  Logger LOGGER = Logger.getLogger(Pickler.class.getName());

  /// Serialize an object to a ByteBuffer
  /// @param buffer The buffer to write to
  /// @param record The object to serialize
  /// @return The number of bytes written
  int serialize(ByteBuffer buffer, T record);

  /// Deserialize an object from a ByteBuffer
  /// @param buffer The buffer to read from
  /// @return The deserialized object
  T deserialize(ByteBuffer buffer);

  /// Calculate the maximum size needed to serialize an object
  /// @param record The object to size
  /// @return The maximum number of bytes needed
  int maxSizeOf(T record);

  /// Factory method for creating a unified pickler for any type
  /// @param clazz The root class (record, enum, or sealed interface)
  /// @return A pickler instance
  static <T> Pickler<T> forClass(Class<T> clazz) {
    Objects.requireNonNull(clazz, "Class must not be null");
    if( !clazz.isRecord() && !clazz.isEnum() && !clazz.isSealed()) {
      throw new IllegalArgumentException("Class must be a record, enum, or sealed interface: " + clazz);
    }
    if(clazz.isInterface() && !clazz.isSealed()) {
      return new PicklerImpl<>(clazz);
    } else if (clazz.isRecord()){
      return new PicklerUsingAst<>(clazz);
    } else if (clazz.isEnum()) {
      return new PicklerImpl<>(clazz);
    } else {
      throw new IllegalArgumentException("Unsupported class type as not a record or a sealed interface: " + clazz);
    }
  }
}
