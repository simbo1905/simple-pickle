// SPDX-FileCopyrightText: 2025 Simon Massey
// SPDX-License-Identifier: MIT

package io.github.simbo1905.no.framework;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Constructor;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.RecordComponent;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static io.github.simbo1905.no.framework.Companion.nameToBasicClass;
import static io.github.simbo1905.no.framework.Constants.INTERNED_NAME;

final class RecordPickler<R extends Record> implements Pickler<R> {
  final MethodHandle[] componentAccessors;
  final Type[][] componentGenericTypes;
  final Class<R> recordClass;
  final int componentCount;
  final MethodHandle canonicalConstructorHandle;
  final InternedName internedName;
  final Map<String, Class<?>> nameToClass = new HashMap<>(nameToBasicClass);
  final Map<Enum<?>, InternedName> enumToName = new HashMap<>();
  final Map<String, Enum<?>> nameToEnum;

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
      Class<?>[] parameterTypes = Arrays.stream(components).map(RecordComponent::getType).toArray(Class<?>[]::new);
      // Record the regular types, the array types, and the enums that we will write into the buffer
      Arrays.stream(parameterTypes).forEach(c -> {
        switch (c) {
          case Class<?> arrayType when arrayType.isArray() -> {
            Class<?> componentType = arrayType.getComponentType();
            nameToClass.putIfAbsent(componentType.getName(), componentType); // FIXME: make short name
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
          default -> nameToClass.putIfAbsent(c.getName(), c); // FIXME make shortName
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
      componentGenericTypes = new Type[components.length][];
      Arrays.setAll(componentGenericTypes, i -> {
        try {
          // FIXME: when we have lists of lists we need to unroll all the generic types to get a nested structure
          return switch (components[i].getGenericType()) {
            case ParameterizedType p -> p.getActualTypeArguments();
            case Type ignored -> null;
          };
        } catch (Throwable e) {
          final var msg = "Failed to resolve component generic type for " + components[i].getName() +
              " in record class " + recordClass.getName() + ": " + e.getClass().getSimpleName();
          LOGGER.severe(() -> msg);
          throw new IllegalArgumentException(msg, e);
        }
      });
      Arrays.stream(componentGenericTypes)
          .filter(Objects::nonNull)
          .flatMap(Arrays::stream)
          .filter(Objects::nonNull)
          .forEach(type -> {
            if (type instanceof Class<?> c) {
              nameToClass.putIfAbsent(c.getName(), c); // FIXME make shortName
            } else if (type instanceof ParameterizedType p) {
              final var genericType = p.getActualTypeArguments();
              for (Type t : genericType) {
                if (t instanceof Class<?> ct) {
                  nameToClass.putIfAbsent(ct.getName(), ct); // FIXME make shortName
                }
              }
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
    nameToEnum = enumToName.entrySet().stream()
        .collect(Collectors.toMap(e -> e.getValue().name(), Map.Entry::getKey));
  }

  void serializeWithMap(WriteBuffer buf, R object, boolean writeName) {
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
    LOGGER.finer(() -> "serializeWithMap object=" + object.hashCode() +
        " position=" + buffer.position() +
        " components=" + components.length +
        " size " + ZigZagEncoding.sizeOf(components.length));
    ZigZagEncoding.putInt(buffer.buffer, components.length);
    // FIXME create array of Function<WriteBuffer, Object, Integer> toWrite = (w, c) -> {}
    IntStream.range(0, components.length).forEach(componentIndex -> {
      final var c = components[componentIndex];
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
        LOGGER.finer(() -> "serializeWithMap writing " + object.getClass().getSimpleName() + " position=" + buffer.position());
        Companion.recursiveWrite(componentIndex, buffer, c);
      }
    });
  }

  @Override
  public int serialize(WriteBuffer buffer, R object) {
    // Validations
    Objects.requireNonNull(buffer);
    if (buffer.isClosed()) {
      throw new IllegalStateException("WriteBuffer is closed");
    }
    if (0 == object.getClass().getRecordComponents().length) {
      throw new AssertionError(object.getClass().getName() + " has no components. Built-in collections conversion to arrays may cause this problem.");
    }
    final var buf = ((WriteBufferImpl) buffer);
    final var startPos = buf.position();
    // Ensure java native endian writes
    buf.buffer.order(java.nio.ByteOrder.BIG_ENDIAN);
    // Initialize the state tracking the offsets of the record class names
    buf.enumToName.putAll(enumToName);
    buf.nameToClass.putAll(nameToClass);
    buf.componentGenericTypes.addAll(Arrays.stream(componentGenericTypes).toList());
    // Write the all the components
    serializeWithMap(buffer, object, false);
    return buffer.position() - startPos;
  }

  @Override
  public R deserialize(ReadBuffer buffer) {
    Objects.requireNonNull(buffer);
    if (buffer.isClosed()) {
      throw new IllegalStateException("PackedBuffer is closed");
    }
    final var buf = ((ReadBufferImpl) buffer);
    buf.buffer.order(java.nio.ByteOrder.BIG_ENDIAN);
    buf.nameToEnum.putAll(nameToEnum);
    buf.nameToClass.putAll(nameToClass);
    buf.componentGenericTypes.addAll(Arrays.stream(componentGenericTypes).toList());
    try {
      return deserializeWithMap(buf, false);
    } catch (RuntimeException e) {
      throw e;
    } catch (Throwable t) {
      throw new RuntimeException("Failed to deserialize " + recordClass.getName() + " : " + t.getMessage(), t);
    }
  }

  R deserializeWithMap(ReadBufferImpl buf, boolean readName) throws Throwable {
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
    final int length = ZigZagEncoding.getInt(buffer);
    LOGGER.finer(() -> "deserializeWithMap read length=" + length + " position=" + buffer.position());
    final Object[] components = new Object[length];
    Arrays.setAll(components, componentIndex -> {
      try {
        // FIXME the record pickler should know if it is recursive or nested so we can skip this work
        // it can also know which ordinal is the one that is the special case of the same type or nested type
        // it can also know the intern name of the nested type
        buffer.mark();
        final var firstByte = buffer.get();
        final var marker = (firstByte == Constants.OPTIONAL_OF.marker()) ? buffer.get() : firstByte;
        if (marker == Constants.SAME_TYPE.marker()) {
          // The component is the same types such as linked list or tree node of other nodes
          return deserializeWithMap(buf, readName);
        } else if (marker == Constants.RECORD.marker()) {
          final InternedName name = (InternedName) Companion.read(componentIndex, buf);
          assert name != null;
          //noinspection unchecked
          Class<? extends Record> concreteType = (Class<? extends Record>) buf.nameToClass.get(name.name());
          Pickler<?> pickler = Pickler.forRecord(concreteType);
          return pickler.deserialize(buf);
        }
        buffer.reset();
        // Read the component
        return Companion.read(componentIndex, buf);
      } catch (Throwable e) {
        final var msg = "Failed to access component: " + componentIndex +
            " in record class '" + recordClass.getName() + "' : " + e.getMessage();
        LOGGER.severe(() -> msg);
        throw new IllegalArgumentException(msg, e);
      }
    });
    //noinspection unchecked
    return (R) canonicalConstructorHandle.invokeWithArguments(components);
  }
}
