// SPDX-FileCopyrightText: 2025 Simon Massey
// SPDX-License-Identifier: Apache-2.0
//
package io.github.simbo1905.no.framework;

import org.jetbrains.annotations.NotNull;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.RecordComponent;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.ToIntFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static io.github.simbo1905.no.framework.PicklerImpl.hashEnumSignature;
import static io.github.simbo1905.no.framework.PicklerImpl.recordClassHierarchy;

final public class PicklerUsingAst<T> implements Pickler<T> {

  public static final String SHA_256 = "SHA-256";
  static final int SAMPLE_SIZE = 32;
  static final int CLASS_SIG_BYTES = Long.BYTES;
  static final CompatibilityMode COMPATIBILITY_MODE =
      CompatibilityMode.valueOf(System.getProperty("no.framework.Pickler.Compatibility", "DISABLED"));
  final Class<?> rootClass; // Root class for this pickler, used for discovery and serialization
  // Global lookup tables indexed by ordinal - the core of the unified architecture
  final Class<?>[] userTypes;     // Lexicographically sorted user types which are subclasses of Record or Enum
  final long[] typeSignatures;    // CLASS_SIG_BYTES SHA256 signatures for backwards compatibility checking
  // Pre-built component metadata arrays for all discovered record types - the core performance optimization:
  final MethodHandle[] recordConstructors;      // Constructor method handles indexed by ordinal
  final MethodHandle[][] componentAccessors;    // [recordOrdinal][componentIndex] -> accessor method handle
  final TypeExpr[][] componentTypeExpressions;       // [recordOrdinal][componentIndex] -> component type AST structure
  final BiConsumer<ByteBuffer, Object>[][] componentWriters;  // [recordOrdinal][componentIndex] -> writer chain of delegating lambda eliminating use of `switch` on write the hot path
  final Function<ByteBuffer, Object>[][] componentReaders;    // [recordOrdinal][componentIndex] -> reader chain of delegating lambda eliminating use of `switch` on read the hot path
  final ToIntFunction<Object>[][] componentSizers; // [recordOrdinal][componentIndex] -> sizer lambda
  final Map<Class<?>, PicklerImpl.TypeInfo> classToTypeInfo;  // Fast user type to ordinal lookup for the write path

  @SuppressWarnings("unchecked")
  public PicklerUsingAst(Class<T> clazz) {
    Map<Class<?>, PicklerImpl.TypeInfo> classToTypeInfo1;
    LOGGER.info(() -> "Creating AST pickler for root class: " + clazz.getName());
    this.rootClass = clazz;

    Set<Class<?>> allReachableClasses = recordClassHierarchy(rootClass, new HashSet<>())
        .filter(cls -> cls.isRecord() || cls.isEnum() || cls.isSealed())
        .collect(Collectors.toSet());

    LOGGER.info(() -> "Discovered " + allReachableClasses.size() + " reachable user types: " +
        allReachableClasses.stream().map(Class::getSimpleName).toList());

    this.userTypes = allReachableClasses.stream()
        .filter(cls -> !cls.isSealed()) // Remove sealed interfaces - they're only for discovery
        .sorted(Comparator.comparing(Class::getName))
        .toArray(Class<?>[]::new);

    LOGGER.fine(() -> "Discovered types with typeOrdinal: " +
        IntStream.range(0, userTypes.length)
            .mapToObj(i -> "[" + i + "]=" + userTypes[i].getName())
            .collect(Collectors.joining(", ")));

    // Pre-allocate metadata arrays for all discovered record types (array-based for O(1) access)
    int numRecordTypes = userTypes.length;
    this.recordConstructors = new MethodHandle[numRecordTypes];
    this.componentAccessors = new MethodHandle[numRecordTypes][];
    this.componentTypeExpressions = new TypeExpr[numRecordTypes][];
    this.componentWriters = (BiConsumer<ByteBuffer, Object>[][]) new BiConsumer[numRecordTypes][];
    this.componentReaders = (Function<ByteBuffer, Object>[][]) new Function[numRecordTypes][];
    this.componentSizers = (ToIntFunction<Object>[][]) new ToIntFunction[numRecordTypes][];
    this.typeSignatures = new long[numRecordTypes];

    IntStream.range(0, numRecordTypes).forEach(ordinal -> {
      Class<?> userClass = userTypes[ordinal];
      if (userClass.isRecord()) {
        try {
          typeSignatures[ordinal] = hashRecordSignature(ordinal, userClass);
          resolveMethodHandles(ordinal, userClass);
          //metaprogramming(ordinal, userClass);
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      } else if (userClass.isEnum()) {
        // Compute enum signature
        typeSignatures[ordinal] = hashEnumSignature(userClass);
        LOGGER.fine(() -> "Computed enum signature for " + userClass.getSimpleName() +
            ": 0x" + Long.toHexString(typeSignatures[ordinal]));
      }
    });
    // Build the ONE HashMap for class->ordinal lookup (O(1) for hot path)
    this.classToTypeInfo = IntStream.range(0, userTypes.length)
        .boxed()
        .collect(Collectors.toMap(i
                -> userTypes[i],
            i -> {
              Class<?> userClass = userTypes[i];
              if (userClass.isEnum()) {
                return new PicklerImpl.TypeInfo(i, typeSignatures[i], userClass.getEnumConstants());
              } else {
                return new PicklerImpl.TypeInfo(i, typeSignatures[i], null);
              }
            }));
    // Finally do the metaprogramming for all record types
    IntStream.range(0, numRecordTypes).forEach(ordinal -> {
      Class<?> userClass = userTypes[ordinal];
      if (userClass.isRecord()) {
        try {
          metaprogramming(ordinal, userClass);
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    });
    LOGGER.info(() -> "PicklerUsingAst construction complete - ready for high-performance serialization");
  }

  long hashRecordSignature(int ordinal, Class<?> recordClass) {
    LOGGER.fine(() -> "hashRecordSignature for ordinal " + ordinal + ": " + recordClass.getSimpleName());
    // Get component accessors and analyze types
    RecordComponent[] components = recordClass.getRecordComponents();
    int numComponents = components.length;
    componentTypeExpressions[ordinal] = new TypeExpr[numComponents];
    IntStream.range(0, numComponents).forEach(i -> {
      RecordComponent component = components[i];
      final TypeExpr typeExpr = TypeExpr.analyzeType(component.getGenericType());
      componentTypeExpressions[ordinal][i] = typeExpr;
    });
    // Compute and store the type signature for this record
    return hashClassSignature(recordClass, components, componentTypeExpressions[ordinal]);
  }


  @SuppressWarnings({"unchecked"})
  void resolveMethodHandles(int ordinal, Class<?> recordClass) throws Exception {
    LOGGER.fine(() -> "Building metadata for ordinal " + ordinal + ": " + recordClass.getSimpleName());
    // Get component accessors and analyze types
    RecordComponent[] components = recordClass.getRecordComponents();
    int numComponents = components.length;
    Class<?>[] parameterTypes = Arrays.stream(components)
        .map(RecordComponent::getType)
        .toArray(Class<?>[]::new);

    var constructor = recordClass.getDeclaredConstructor(parameterTypes);
    recordConstructors[ordinal] = MethodHandles.lookup().unreflectConstructor(constructor);
    componentAccessors[ordinal] = new MethodHandle[numComponents];

    IntStream.range(0, numComponents).forEach(i -> {
      RecordComponent component = components[i];
      try {
        componentAccessors[ordinal][i] = MethodHandles.lookup().unreflect(component.getAccessor());
      } catch (IllegalAccessException e) {
        throw new RuntimeException("Failed to un reflect accessor for " + component.getName(), e);
      }
    });
  }


  @SuppressWarnings({"unchecked"})
  void metaprogramming(int ordinal, Class<?> recordClass) throws Exception {
    LOGGER.fine(() -> "Building metadata for ordinal " + ordinal + ": " + recordClass.getSimpleName());
    // Get component accessors and analyze types
    RecordComponent[] components = recordClass.getRecordComponents();
    int numComponents = components.length;

    Class<?>[] parameterTypes = Arrays.stream(components)
        .map(RecordComponent::getType)
        .toArray(Class<?>[]::new);
    var constructor = recordClass.getDeclaredConstructor(parameterTypes);
    recordConstructors[ordinal] = MethodHandles.lookup().unreflectConstructor(constructor);
    componentAccessors[ordinal] = new MethodHandle[numComponents];
    componentWriters[ordinal] = (BiConsumer<ByteBuffer, Object>[]) new BiConsumer[numComponents];
    componentReaders[ordinal] = (Function<ByteBuffer, Object>[]) new Function[numComponents];
    componentSizers[ordinal] = (ToIntFunction<Object>[]) new ToIntFunction[numComponents];

    IntStream.range(0, numComponents).forEach(i -> {
      RecordComponent component = components[i];
      try {
        componentAccessors[ordinal][i] = MethodHandles.lookup().unreflect(component.getAccessor());
      } catch (IllegalAccessException e) {
        throw new RuntimeException("Failed to un reflect accessor for " + component.getName(), e);
      }
      final var typeExpr = componentTypeExpressions[ordinal][i];
      final var accessor = componentAccessors[ordinal][i];
      // Build writer, reader, and sizer chains
      componentWriters[ordinal][i] = buildWriterChain(typeExpr, accessor);
      componentReaders[ordinal][i] = buildReaderChain(typeExpr);
      componentSizers[ordinal][i] = buildSizerChain(typeExpr, accessor);
    });

    LOGGER.fine(() -> "Completed metaprogramming for " + recordClass.getSimpleName() + " with " + numComponents + " components");
  }

  BiConsumer<ByteBuffer, Object> buildWriterChain(TypeExpr typeExpr, MethodHandle methodHandle) {
    if (typeExpr.isPrimitive()) {
      // For primitive types, we can directly write the value using the method handle
      LOGGER.fine(() -> "Building writer chain for primitive type: " + typeExpr.toTreeString());
      final var primitiveType = ((TypeExpr.PrimitiveValueNode) typeExpr).type();
      return buildPrimitiveValueWriter(primitiveType, methodHandle);
    } else {
      return (ByteBuffer buffer, Object record) -> {
        throw new AssertionError("not implemented: " + typeExpr.toTreeString() +
            " for record: " + record.getClass().getSimpleName() + " with method handle: " + methodHandle);
      };
    }
  }

  Function<ByteBuffer, Object> buildReaderChain(TypeExpr typeExpr) {
    if (typeExpr.isPrimitive()) {
      LOGGER.fine(() -> "Building reader chain for primitive type: " + typeExpr.toTreeString());
      final var primitiveType = ((TypeExpr.PrimitiveValueNode) typeExpr).type();
      return buildPrimitiveValueReader(primitiveType);
    } else {
      return (ByteBuffer buffer) -> {
        throw new AssertionError("not implemented: " + typeExpr.toTreeString() +
            " for record with method handle: ");
      };
    }
  }


  ToIntFunction<Object> buildSizerChain(TypeExpr typeExpr, MethodHandle methodHandle) {
    if (typeExpr.isPrimitive()) {
      LOGGER.fine(() -> "Building sizer chain for primitive type: " + typeExpr.toTreeString());
      final var primitiveType = ((TypeExpr.PrimitiveValueNode) typeExpr).type();
      return buildPrimitiveValueSizer(primitiveType, methodHandle);
    } else {
      return (Object record) -> {
        throw new AssertionError("not implemented: " + typeExpr.toTreeString() +
            " for record with method handle: " + methodHandle);
      };
    }
  }

  /// Compute a CLASS_SIG_BYTES signature from class name and component metadata
  static long hashClassSignature(Class<?> clazz, RecordComponent[] components, TypeExpr[] componentTypes) {
    try {
      MessageDigest digest = MessageDigest.getInstance(SHA_256);

      String input = Stream.concat(
          Stream.of(clazz.getSimpleName()),
          IntStream.range(0, components.length)
              .boxed()
              .flatMap(i -> Stream.concat(
                  Stream.of(componentTypes[i].toTreeString()),
                  Stream.of(components[i].getName())
              ))
      ).collect(Collectors.joining("!"));

      byte[] hash = digest.digest(input.getBytes(StandardCharsets.UTF_8));

      // Convert first CLASS_SIG_BYTES to long
      //      Byte Index:   0       1       2        3        4        5        6        7
      //      Bits:      [56-63] [48-55] [40-47] [32-39] [24-31] [16-23] [ 8-15] [ 0-7]
      //      Shift:      <<56   <<48   <<40    <<32    <<24    <<16    <<8     <<0
      return IntStream.range(0, CLASS_SIG_BYTES)
          .mapToLong(i -> (hash[i] & 0xFFL) << (56 - i * 8))
          .reduce(0L, (a, b) -> a | b);
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException(SHA_256 + " not available", e);
    }
  }

  static @NotNull BiConsumer<ByteBuffer, Object> buildPrimitiveValueWriter(TypeExpr.PrimitiveValueType primitiveType, MethodHandle methodHandle) {
    return switch (primitiveType) {
      case BOOLEAN -> {
        LOGGER.fine(() -> "Building writer chain for boolean.class primitive type");
        yield (ByteBuffer buffer, Object record) -> {
          try {
            final var result = methodHandle.invokeWithArguments(record);
            buffer.put((byte) ((boolean) result ? 1 : 0));
          } catch (Throwable e) {
            throw new RuntimeException("Failed to write boolean value", e);
          }
        };
      }
      case BYTE -> {
        LOGGER.fine(() -> "Building writer chain for byte.class primitive type");
        yield (ByteBuffer buffer, Object record) -> {
          try {
            buffer.put((byte) methodHandle.invokeWithArguments(record));
          } catch (Throwable e) {
            throw new RuntimeException(e.getMessage(), e);
          }
        };
      }
      case SHORT -> {
        LOGGER.fine(() -> "Building writer chain for short.class primitive type");
        yield (ByteBuffer buffer, Object record) -> {
          try {
            buffer.putShort((short) methodHandle.invokeWithArguments(record));
          } catch (Throwable e) {
            throw new RuntimeException(e.getMessage(), e);
          }
        };
      }
      case CHARACTER -> {
        LOGGER.fine(() -> "Building writer chain for char.class primitive type");
        yield (ByteBuffer buffer, Object record) -> {
          try {
            buffer.putChar((char) methodHandle.invokeWithArguments(record));
          } catch (Throwable e) {
            throw new RuntimeException(e.getMessage(), e);
          }
        };
      }
      case INTEGER -> {
        LOGGER.fine(() -> "Building writer chain for int.class primitive type");
        yield (ByteBuffer buffer, Object record) -> {
          try {
            Object result = methodHandle.invokeWithArguments(record);
            buffer.putInt((int) result);
          } catch (Throwable e) {
            throw new RuntimeException(e.getMessage(), e);
          }
        };
      }
      case LONG -> {
        LOGGER.fine(() -> "Building writer chain for long.class primitive type");
        yield (ByteBuffer buffer, Object record) -> {
          try {
            buffer.putLong((long) methodHandle.invokeWithArguments(record));
          } catch (Throwable e) {
            throw new RuntimeException(e.getMessage(), e);
          }
        };
      }
      case FLOAT -> {
        LOGGER.fine(() -> "Building writer chain for float.class primitive type");
        yield (ByteBuffer buffer, Object record) -> {
          try {
            buffer.putFloat((float) methodHandle.invokeWithArguments(record));
          } catch (Throwable e) {
            throw new RuntimeException(e.getMessage(), e);
          }
        };
      }
      case DOUBLE -> {
        LOGGER.fine(() -> "Building writer chain for double.class primitive type");
        yield (ByteBuffer buffer, Object record) -> {
          try {
            buffer.putDouble((double) methodHandle.invokeWithArguments(record));
          } catch (Throwable e) {
            throw new RuntimeException(e.getMessage(), e);
          }
        };
      }
    };
  }

  static @NotNull Function<ByteBuffer, Object> buildPrimitiveValueReader(TypeExpr.PrimitiveValueType primitiveType) {
    return switch (primitiveType) {
      case BOOLEAN -> (buffer) -> buffer.get() != 0;
      case BYTE -> ByteBuffer::get;
      case SHORT -> ByteBuffer::getShort;
      case CHARACTER -> ByteBuffer::getChar;
      case INTEGER -> ByteBuffer::getInt;
      case LONG -> ByteBuffer::getLong;
      case FLOAT -> ByteBuffer::getFloat;
      case DOUBLE -> ByteBuffer::getDouble;
    };
  }

  static @NotNull ToIntFunction<Object> buildPrimitiveValueSizer(TypeExpr.PrimitiveValueType primitiveType, MethodHandle ignored) {
    return switch (primitiveType) {
      case BOOLEAN, BYTE -> (Object record) -> Byte.BYTES;
      case SHORT -> (Object record) -> Short.BYTES;
      case CHARACTER -> (Object record) -> Character.BYTES;
      case INTEGER -> (Object record) -> Integer.BYTES;
      case LONG -> (Object record) -> Long.BYTES;
      case FLOAT -> (Object record) -> Float.BYTES;
      case DOUBLE -> (Object record) -> Double.BYTES;
    };
  }

  /// Build a writer for an Enum type
  /// This has to write out the typeOrdinal first as they would be more than one enum type in the system
  /// Then it writes out the typeSignature for the enum class
  /// Finally, it writes out the ordinal of the enum constant
  static @NotNull BiConsumer<ByteBuffer, Object> buildEnumWriter(
      final Map<Class<?>, PicklerImpl.TypeInfo> classToTypeInfo,
      final MethodHandle methodHandle) {
    LOGGER.fine(() -> "Building writer chain for Enum");
    return (ByteBuffer buffer, Object record) -> {
      try {
        Enum<?> enumValue = (Enum<?>) methodHandle.invokeWithArguments(record);
        final var typeInfo = classToTypeInfo.get(enumValue.getDeclaringClass());
        ZigZagEncoding.putInt(buffer, typeInfo.typeOrdinal());
        ZigZagEncoding.putLong(buffer, typeInfo.typeSignature());
        int ordinal = enumValue.ordinal();
        ZigZagEncoding.putInt(buffer, ordinal);
      } catch (Throwable e) {
        throw new RuntimeException("Failed to write Enum: " + e.getMessage(), e);
      }
    };
  }

  static @NotNull BiConsumer<ByteBuffer, Object> buildValueWriter(
      final TypeExpr.RefValueType refValueType,
      final MethodHandle methodHandle) {
    return switch (refValueType) {
      case BOOLEAN -> {
        LOGGER.fine(() -> "Building writer chain for boolean.class primitive type");
        yield (ByteBuffer buffer, Object record) -> {
          try {
            final var result = methodHandle.invokeWithArguments(record);
            buffer.put((byte) ((boolean) result ? 1 : 0));
          } catch (Throwable e) {
            throw new RuntimeException("Failed to write boolean value", e);
          }
        };
      }
      case BYTE -> {
        LOGGER.fine(() -> "Building writer chain for byte.class primitive type");
        yield (ByteBuffer buffer, Object record) -> {
          try {
            buffer.put((byte) methodHandle.invokeWithArguments(record));
          } catch (Throwable e) {
            throw new RuntimeException(e.getMessage(), e);
          }
        };
      }
      case UUID -> {
        LOGGER.fine(() -> "Building writer chain for UUID");
        yield (ByteBuffer buffer, Object record) -> {
          try {
            UUID uuid = (UUID) methodHandle.invokeWithArguments(record);
            buffer.putLong(uuid.getMostSignificantBits());
            buffer.putLong(uuid.getLeastSignificantBits());
          } catch (Throwable e) {
            throw new RuntimeException("Failed to write UUID: " + e.getMessage(), e);
          }
        };
      }
      case SHORT -> {
        LOGGER.fine(() -> "Building writer chain for short.class primitive type");
        yield (ByteBuffer buffer, Object record) -> {
          try {
            buffer.putShort((short) methodHandle.invokeWithArguments(record));
          } catch (Throwable e) {
            throw new RuntimeException(e.getMessage(), e);
          }
        };
      }
      case CHARACTER -> {
        LOGGER.fine(() -> "Building writer chain for char.class primitive type");
        yield (ByteBuffer buffer, Object record) -> {
          try {
            buffer.putChar((char) methodHandle.invokeWithArguments(record));
          } catch (Throwable e) {
            throw new RuntimeException(e.getMessage(), e);
          }
        };
      }
      case INTEGER -> {
        LOGGER.fine(() -> "Building writer chain for int.class primitive type");
        yield (ByteBuffer buffer, Object record) -> {
          try {
            Object result = methodHandle.invokeWithArguments(record);
            buffer.putInt((int) result);
          } catch (Throwable e) {
            throw new RuntimeException(e.getMessage(), e);
          }
        };
      }
      case LONG -> {
        LOGGER.fine(() -> "Building writer chain for long.class primitive type");
        yield (ByteBuffer buffer, Object record) -> {
          try {
            buffer.putLong((long) methodHandle.invokeWithArguments(record));
          } catch (Throwable e) {
            throw new RuntimeException(e.getMessage(), e);
          }
        };
      }
      case FLOAT -> {
        LOGGER.fine(() -> "Building writer chain for float.class primitive type");
        yield (ByteBuffer buffer, Object record) -> {
          try {
            buffer.putFloat((float) methodHandle.invokeWithArguments(record));
          } catch (Throwable e) {
            throw new RuntimeException(e.getMessage(), e);
          }
        };
      }
      case DOUBLE -> {
        LOGGER.fine(() -> "Building writer chain for double.class primitive type");
        yield (ByteBuffer buffer, Object record) -> {
          try {
            buffer.putDouble((double) methodHandle.invokeWithArguments(record));
          } catch (Throwable e) {
            throw new RuntimeException(e.getMessage(), e);
          }
        };
      }
      case STRING -> {
        LOGGER.fine(() -> "Building writer chain for String");
        yield (ByteBuffer buffer, Object record) -> {
          try {
            String str = (String) methodHandle.invokeWithArguments(record);
            byte[] bytes = str.getBytes(StandardCharsets.UTF_8);
            ZigZagEncoding.putInt(buffer, bytes.length);
            buffer.put(bytes);
          } catch (Throwable e) {
            throw new RuntimeException("Failed to write String: " + e.getMessage(), e);
          }
        };
      }
      default -> throw new AssertionError("not implemented yet ref value type: " + refValueType);
    };
  }

  static @NotNull Function<ByteBuffer, Object> buildEnumReader(
      final Map<Class<?>, PicklerImpl.TypeInfo> classToTypeInfo) {
    LOGGER.fine(() -> "Building reader chain for Enum");
    return (ByteBuffer buffer) -> {
      try {
        int typeOrdinal = ZigZagEncoding.getInt(buffer);
        long typeSignature = ZigZagEncoding.getLong(buffer);
        Class<?> enumClass = classToTypeInfo.entrySet().stream()
            .filter(e -> e.getValue().typeOrdinal() == typeOrdinal && e.getValue().typeSignature() == typeSignature)
            .map(Map.Entry::getKey)
            .findFirst()
            .orElseThrow(() -> new IllegalArgumentException("Unknown enum type ordinal: " + typeOrdinal +
                " with signature: " + Long.toHexString(typeSignature)));

        int ordinal = ZigZagEncoding.getInt(buffer);
        return enumClass.getEnumConstants()[ordinal];
      } catch (Throwable e) {
        throw new RuntimeException("Failed to read Enum: " + e.getMessage(), e);
      }
    };
  }

  static @NotNull Function<ByteBuffer, Object> buildValueReader(TypeExpr.RefValueType valueType) {
    return switch (valueType) {
      case BOOLEAN -> (buffer) -> buffer.get() != 0;
      case BYTE -> ByteBuffer::get;
      case SHORT -> ByteBuffer::getShort;
      case CHARACTER -> ByteBuffer::getChar;
      case INTEGER -> ByteBuffer::getInt;
      case LONG -> ByteBuffer::getLong;
      case FLOAT -> ByteBuffer::getFloat;
      case DOUBLE -> ByteBuffer::getDouble;
      case UUID -> (buffer) -> {
        long most = buffer.getLong();
        long least = buffer.getLong();
        return new UUID(most, least);
      };
      case STRING -> (buffer) -> {
        int length = ZigZagEncoding.getInt(buffer);
        byte[] bytes = new byte[length];
        buffer.get(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
      };
      default -> throw new AssertionError("not implemented yet ref value type: " + valueType);
    };
  }

  static @NotNull ToIntFunction<Object> buildValueSizer(TypeExpr.RefValueType refValueType, MethodHandle accessor) {
    return switch (refValueType) {
      case BOOLEAN, BYTE -> (Object record) -> Byte.BYTES;
      case SHORT -> (Object record) -> Short.BYTES;
      case CHARACTER -> (Object record) -> Character.BYTES;
      case INTEGER -> (Object record) -> Integer.BYTES;
      case LONG -> (Object record) -> Long.BYTES;
      case FLOAT -> (Object record) -> Float.BYTES;
      case DOUBLE -> (Object record) -> Double.BYTES;
      case UUID -> (Object record) -> 2 * Long.BYTES; // UUID is two longs
      case ENUM -> (Object record) -> Integer.BYTES + 2 * Long.BYTES; // typeOrdinal + typeSignature + enum ordinal
      case STRING -> (Object record) -> {
        try {
          // Worst case estimate users the length then one int per UTF-8 encoded character
          String str = (String) accessor.invokeWithArguments(record);
          return (str.length() + 1) * Integer.BYTES;
        } catch (Throwable e) {
          throw new RuntimeException("Failed to size String", e);
        }
      };
      case RECORD -> (Object record) -> 0;
      case INTERFACE -> (Object userType) -> {
        if (userType instanceof Enum<?> ignored) {
          // For enums, we store the ordinal and type signature
          return Integer.BYTES + Long.BYTES;
        } else if (userType instanceof Record record) {
          return 0;
        } else {
          throw new IllegalArgumentException("Unsupported interface type: " + userType.getClass().getSimpleName());
        }
      };
    };
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
