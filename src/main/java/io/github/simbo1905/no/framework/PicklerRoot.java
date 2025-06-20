// SPDX-FileCopyrightText: 2025 Simon Massey
// SPDX-License-Identifier: Apache-2.0
//
package io.github.simbo1905.no.framework;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

final class PicklerRoot<T> implements Pickler<T> {

  @SuppressWarnings("rawtypes")
  public static final Map<Class, Pickler> REGISTRY = new ConcurrentHashMap<>();

  final Class<?>[] userTypes;
  final RecordPickler<?>[] picklers;

  public PicklerRoot(Class<?>[] sortedUserTypes) {
    this.userTypes = sortedUserTypes;
    this.picklers = new RecordPickler<?>[sortedUserTypes.length];
    for (Class<?> clazz : sortedUserTypes) {
      REGISTRY.computeIfAbsent(clazz, aClass -> {
        //noinspection rawtypes,unchecked
        return new RecordPickler(aClass, sortedUserTypes);
      });
    }
    LOGGER.info(() -> "PicklerRoot construction complete - ready for high-performance serialization");
  }

  @Override
  public int serialize(ByteBuffer buffer, T record) {
    return 0;
  }

  @Override
  public T deserialize(ByteBuffer buffer) {
    return null;
  }

  @Override
  public int maxSizeOf(T record) {
    return 0;
  }
}
