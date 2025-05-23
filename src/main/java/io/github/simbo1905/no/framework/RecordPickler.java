// SPDX-FileCopyrightText: 2025 Simon Massey
// SPDX-License-Identifier: MIT

package io.github.simbo1905.no.framework;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Constructor;
import java.lang.reflect.RecordComponent;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.stream.IntStream;

import static io.github.simbo1905.no.framework.Companion.nameToBasicClass;
import static io.github.simbo1905.no.framework.Constants.INTERNED_NAME;

final class RecordPickler<R extends Record> implements Pickler<R> {
  final MethodHandle[] componentAccessors;
  final Class<R> recordClass;
  final int componentCount;
  final MethodHandle canonicalConstructorHandle;
  final InternedName internedName;
  final Map<String, Class<?>> nameToClass = new HashMap<>(nameToBasicClass);
  final Map<Enum<?>, InternedName> enumToName = new HashMap<>();
  final Map<InternedName, Enum<?>> nameToEnum;

  RecordPickler(final Class<R> recordClass,
                InternedName internedName,
                final Map<String, Class<?>> classesByShortName
  ) {
    Objects.requireNonNull(recordClass);
    Objects.requireNonNull(internedName);
    Objects.requireNonNull(classesByShortName);
    nameToClass.putAll(classesByShortName);
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
      // Record the regular types, the array types, and the enums that we will write into the buffer
      Arrays.stream(parameterTypes).forEach(c -> {
        switch (c) {
          case Class<?> arrayType when arrayType.isArray() -> {
            Class<?> componentType = arrayType.getComponentType();
            nameToClass.putIfAbsent(componentType.getName(), componentType);
          }
          case Class<?> enumType when enumType.isEnum() -> {
            for (Object e : enumType.getEnumConstants()) {
              if (e instanceof Enum<?> enumConst) {
                final var shortName = IntStream.range(0, Math.min(enumType.getName().length(), recordClass.getName().length()))
                    .takeWhile(i -> enumType.getName().charAt(i) == recordClass.getName().charAt(i))
                    .reduce((a, b) -> b)
                    .stream()
                    .mapToObj(i -> enumType.getName().substring(i + 1) + "." + enumConst.name())
                    .findFirst()
                    .orElse(enumType.getName() + "." + enumConst.name());
                enumToName.put(enumConst, new InternedName(shortName));
              }
            }
          }
          default -> nameToClass.putIfAbsent(c.getName(), c);
        }
      });
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
    // flip the map for the deserialization
    nameToEnum = enumToName.entrySet().stream().collect(java.util.stream.Collectors.toMap(Map.Entry::getValue, Map.Entry::getKey));
  }

  void serializeWithMap(WriteBuffer buf, R object, boolean writeName) {
    if (buf.isClosed()) {
      throw new IllegalStateException("PackedBuffer is closed");
    }
    final var buffer = ((WriteBufferImpl) buf);
    if (writeName) {
      // If we are being asked to write out our record class name by a sealed pickler then we so now
      buffer.offsetMap.put(internedName, new InternedPosition(buffer.position()));
      WriteBufferImpl.write(buffer.buffer, internedName);
    }
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
    LOGGER.finer(() -> "serializeWithMap object=" + object.hashCode() + " writing component length length=" + components.length + " position=" + buffer.position() + " writing length as zigzag size " + ZigZagEncoding.sizeOf(components.length));
    ZigZagEncoding.putInt(buffer.buffer, components.length);
    Arrays.stream(components).forEach(c -> {
      if (c instanceof Record record) {
        if (recordClass.equals(record.getClass())) {
          LOGGER.fine(() -> "serializeWithMap writing SAME_TYPE type record " + record.getClass().getName() + " position=" + buffer.position());
          // If the record is the same class as this pickler we simply mark it is the same pickler type as self and recurse
          buffer.buffer.put(Constants.SAME_TYPE.marker());
          //noinspection unchecked
          serializeWithMap(buffer, (R) record, false);
        } else {
          LOGGER.fine(() -> "serializeWithMap writing RECORD type record " + record.getClass().getName() + " position=" + buffer.position());
          // we need to write that this is a different record type we need to resolve the pickler for
          buffer.buffer.put(Constants.RECORD.marker());
          // if the record is a different class we need to write out the interned name
          @SuppressWarnings("unchecked")
          RecordPickler<Record> nestedPickler = (RecordPickler<Record>) Pickler.forRecord(record.getClass());
          nestedPickler.serializeWithMap(buffer, record, true);
        }
      } else {
        Companion.recursiveWrite(buffer, c);
      }
    });
  }

  @Override
  public void serialize(WriteBuffer buf, R object) {
    // Null checks
    Objects.requireNonNull(buf);
    Objects.requireNonNull(object);
    final var buffer = ((WriteBufferImpl) buf);
    // Use java native endian for float and double writes
    buffer.buffer.order(java.nio.ByteOrder.BIG_ENDIAN);
    if (0 == object.getClass().getRecordComponents().length) {
      throw new AssertionError(object.getClass().getName() + " has no components. Built-in collections conversion to arrays may cause this problem.");
    }
    // Write the all the components
    serializeWithMap(buf, object, false);
  }

  @Override
  public R deserialize(ReadBuffer buf) {
    if (buf.isClosed()) {
      throw new IllegalStateException("PackedBuffer is closed");
    }
    try {
      return deserializeWithMap(nameToClass, (ReadBufferImpl) buf, false);
    } catch (RuntimeException e) {
      throw e;
    } catch (Throwable t) {
      throw new RuntimeException("Failed to deserialize " + recordClass.getName() + " : " + t.getMessage(), t);
    }
  }

  R deserializeWithMap(final Map<String, Class<?>> nameToRecordClass, ReadBufferImpl buf, boolean readName) throws Throwable {
    final var buffer = buf.buffer;
    if (readName) {
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
    }
    // Read the number of components as an unsigned byte
    LOGGER.finer(() -> "deserializeWithMap reading component length position=" + buffer.position());
    final int length = ZigZagEncoding.getInt(buffer);
    final Object[] components = new Object[length];
    Arrays.setAll(components, i -> {
      try {
        buffer.mark();
        final var firstByte = buffer.get();
        final var marker = (firstByte == Constants.OPTIONAL_OF.marker()) ? buffer.get() : firstByte;
        if (marker == Constants.SAME_TYPE.marker()) {
          // The component is the same types such as linked list or tree node of other nodes
          return deserializeWithMap(nameToRecordClass, buf, readName);
        } else if (marker == Constants.RECORD.marker()) {
          final InternedName name = (InternedName) Companion.read(nameToRecordClass, buffer);
          assert name != null;
          //noinspection unchecked
          Class<? extends Record> concreteType = (Class<? extends Record>) nameToRecordClass.get(name.name());
          Pickler<?> pickler = Pickler.forRecord(concreteType);
          return pickler.deserialize(buf);
        }
        buffer.reset();
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
