// SPDX-FileCopyrightText: 2025 Simon Massey
// SPDX-License-Identifier: MIT

package io.github.simbo1905.no.framework;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static io.github.simbo1905.no.framework.Companion.nameToBasicClass;
import static io.github.simbo1905.no.framework.Pickler.LOGGER;
import static io.github.simbo1905.no.framework.Constants.*;

class SealedPickler<S>  {
   Map<Class<? extends S>, Pickler<? extends S>> subPicklers;
   Map<String, Class<?>> recordClassByName;
   Map<String, Class<?>> nameToRecordClass = new HashMap<>(nameToBasicClass);
  
  // Combined class name mappings from all delegatee RecordPicklers
   ClassNameMappings combinedClassNameMappings;

  public SealedPickler(
      Map<Class<? extends S>, Pickler<? extends S>> subPicklers,
      Map<String, Class<?>> classesByShortName) {
    LOGGER.info("Creating SealedPickler for " + subPicklers.size() + " delegate types: " + 
        subPicklers.keySet().stream().map(Class::getSimpleName).collect(Collectors.toList()));
    this.subPicklers = subPicklers;
    this.recordClassByName = classesByShortName;
    // Add both record and enum classes to nameToRecordClass for class name resolution
    this.nameToRecordClass.putAll(classesByShortName);
    
    // Collect ClassNameMappings from all delegatee RecordPicklers and merge them
//    ClassNameMappings[] mappingsArray = subPicklers.values().stream()
//        .filter(pickler -> pickler instanceof RecordPickler<?>)
//        .map(pickler -> ((RecordPickler<?>) pickler).getClassNameMappings())
//        .toArray(ClassNameMappings[]::new);
    
    // Create additional mappings for enum classes that don't have picklers
    Map<Class<?>, String> enumClassMappings = classesByShortName.entrySet().stream()
        .filter(e -> e.getValue().isEnum())
        .collect(Collectors.toMap(
            Map.Entry::getValue,
            Map.Entry::getKey
        ));
    
    Map<String, Class<?>> enumNameMappings = classesByShortName.entrySet().stream()
        .filter(e -> e.getValue().isEnum())
        .collect(Collectors.toMap(
            Map.Entry::getKey,
            Map.Entry::getValue
        ));
    
    ClassNameMappings enumMappings = new ClassNameMappings(enumClassMappings, enumNameMappings);
    
    // TODO revert to FINER logging after bug fix
    LOGGER.info(() -> "SealedPickler: collected " + enumClassMappings.size() + " enum class mappings");
    
//    // Merge record pickler mappings with enum mappings
//    ClassNameMappings[] allMappings = new ClassNameMappings[mappingsArray.length + 1];
//    System.arraycopy(mappingsArray, 0, allMappings, 0, mappingsArray.length);
//    allMappings[mappingsArray.length] = enumMappings;
//
//    this.combinedClassNameMappings = ClassNameMappings.merge(allMappings);
    // TODO revert to FINER logging after bug fix
//    LOGGER.info(() -> "SealedPickler: final combined class mappings: " +
//        combinedClassNameMappings.classToInternedName().entrySet().stream()
//            .map(e -> e.getKey().getSimpleName() + "->" + e.getValue())
//            .collect(Collectors.toList()));
  }

  /// Serialize sealed interface permit - either a record or an enum.
  public int serialize(WriteBuffer buffer, S object) {
    Objects.requireNonNull(buffer, "buffer");
    if (buffer.isClosed()) {
      throw new IllegalStateException("Cannot serialize to a closed buffer");
    }
    final var buf = (WriteBufferImpl) buffer;
    final var startPosition = buf.position();
    LOGGER.finer(() -> "SealedPickler.serialize: start position=" + startPosition + " object=" + object);
    if (object == null) {
      buf.put(NULL.marker());
      return 1; // 1 byte for NULL marker
    }
    //noinspection unchecked
    Class<? extends S> concreteType = (Class<? extends S>) object.getClass();
    LOGGER.finer(() -> "SealedPickler.serialize: concrete type=" + concreteType + " isRecord=" + concreteType.isRecord() + " isEnum=" + concreteType.isEnum());
    
    if (concreteType.isRecord()) {
      // Handle record permit - write RECORD marker and delegate to RecordPickler
      LOGGER.finer(() -> "SealedPickler.serialize: writing RECORD marker for " + concreteType.getSimpleName());
      buf.put(RECORD.marker());
      Pickler<?> pickler = subPicklers.get(concreteType);
      if (pickler == null) {
        throw new IllegalStateException("No pickler found for record type: " + concreteType.getName());
      }
      Writers.writeCompressedClassName(buf, object.getClass());
      //Companion.serializeWithPickler(buf, pickler, object);
    } else if (concreteType.isEnum()) {
      // Handle enum permit - write ENUM marker and serialize directly without class name compression
      LOGGER.finer(() -> "SealedPickler.serialize: writing ENUM marker for " + concreteType.getSimpleName());
      buf.put(ENUM.marker());
      
      // Write enum class name directly (no compression needed since we have class mappings)
      String className = combinedClassNameMappings.classToInternedName().get(concreteType);
      if (className == null) {
        throw new IllegalStateException("No class name mapping found for enum: " + concreteType.getName());
      }
      byte[] classNameBytes = className.getBytes(java.nio.charset.StandardCharsets.UTF_8);
      ZigZagEncoding.putInt(buf.buffer, classNameBytes.length);
      buf.buffer.put(classNameBytes);
      
      // Write enum constant name
      Enum<?> enumValue = (Enum<?>) object;
      String constantName = enumValue.name();
      byte[] constantBytes = constantName.getBytes(java.nio.charset.StandardCharsets.UTF_8);
      ZigZagEncoding.putInt(buf.buffer, constantBytes.length);
      buf.buffer.put(constantBytes);
    } else {
      throw new IllegalArgumentException("Unsupported permit type: " + concreteType.getName() + 
          " (must be record or enum)");
    }
    return buf.position() - startPosition;
  }

  public S deserialize(ReadBuffer readBuffer) {
    final var buf = (ReadBufferImpl) readBuffer;
    final var buffer = buf.buffer;
    LOGGER.finer(() -> "SealedPickler.deserialize: readBuffer parentReflection=" + (buf.parentReflection != null ? "present" : "null"));
    buffer.order(java.nio.ByteOrder.BIG_ENDIAN);
    buffer.mark();
    final byte marker = buffer.get();
    LOGGER.finer(() -> "SealedPickler.deserialize: read marker=" + marker + " NULL=" + NULL.marker() + " RECORD=" + RECORD.marker() + " ENUM=" + ENUM.marker());
    if (marker == NULL.marker()) {
      return null;
    }
    buffer.reset();
    LOGGER.finer(() -> "SealedPickler.deserialize: buffer reset, position=" + buffer.position());
    buf.nameToClass.putAll(nameToRecordClass);
    LOGGER.finer(() -> "SealedPickler.deserialize: nameToClass size=" + buf.nameToClass.size() + " entries=" + buf.nameToClass.keySet());
    
    if (marker == RECORD.marker()) {
      // Handle record permit
      buffer.get(); // consume RECORD marker
      LOGGER.finer(() -> "SealedPickler.deserialize: deserializing RECORD permit");
      Class<?> clazz = Writers.readCompressedClassName(buf);
      final RecordPickler<?> pickler = null;// (RecordPickler<?>) subPicklers.get(clazz);
      if (pickler == null) {
        throw new IllegalStateException("No pickler found for record " + clazz.getName() + " in sealed hierarchy: " +
            String.join(",", this.recordClassByName.keySet()));
      }
      try {
        //noinspection unchecked
        return (S) null;
      } catch (RuntimeException e) {
        throw e;
      } catch (Throwable t) {
        throw new RuntimeException("Failed to deserialize record " + clazz.getName() + " : " + t.getMessage(), t);
      }
    } else if (marker == ENUM.marker()) {
      // Handle enum permit - consume ENUM marker and deserialize directly
      buffer.get(); // consume ENUM marker
      LOGGER.finer(() -> "SealedPickler.deserialize: deserializing ENUM permit, buffer position=" + buffer.position());
      try {
        // Read enum class name
        int classNameLength = ZigZagEncoding.getInt(buffer);
        byte[] classNameBytes = new byte[classNameLength];
        buffer.get(classNameBytes);
        String className = new String(classNameBytes, java.nio.charset.StandardCharsets.UTF_8);
        
        // Resolve enum class
        Class<?> enumClass = combinedClassNameMappings.shortNameToClass().get(className);
        if (enumClass == null) {
          throw new IllegalStateException("No enum class found for name: " + className);
        }
        
        // Read enum constant name
        int constantLength = ZigZagEncoding.getInt(buffer);
        byte[] constantBytes = new byte[constantLength];
        buffer.get(constantBytes);
        String constantName = new String(constantBytes, java.nio.charset.StandardCharsets.UTF_8);
        
        // Get enum constant
        @SuppressWarnings("unchecked")
        Class<Enum> enumType = (Class<Enum>) enumClass;
        Enum<?> enumValue = Enum.valueOf(enumType, constantName);
        
        //noinspection unchecked
        return (S) enumValue;
      } catch (RuntimeException e) {
        throw e;
      } catch (Throwable t) {
        throw new RuntimeException("Failed to deserialize enum: " + t.getMessage(), t);
      }
    } else {
      throw new IllegalStateException("Unexpected marker in sealed interface: " + marker + 
          " (expected RECORD=" + RECORD.marker() + " or ENUM=" + ENUM.marker() + ")");
    }
  }

  public WriteBuffer allocateForWriting(int size) {
    return new WriteBufferImpl(
        ByteBuffer.allocate(size),
        combinedClassNameMappings.classToInternedName()::get
    );
  }

  public WriteBuffer wrapForWriting(ByteBuffer buf) {
    return new WriteBufferImpl(
        buf,
        combinedClassNameMappings.classToInternedName()::get
    );
  }

  public int maxSizeOf(S record) {
    if (record == null) {
      return 1; // NULL marker
    }
    @SuppressWarnings("unchecked")
    Class<? extends S> concreteType = (Class<? extends S>) record.getClass();
    
    if (concreteType.isRecord()) {
      // Handle record permit - delegate to RecordPickler
      Pickler<? extends S> pickler = subPicklers.get(concreteType);
      if (pickler == null) {
        throw new IllegalArgumentException("No pickler found for record type: " + concreteType.getName());
      }
      @SuppressWarnings("unchecked")
      Pickler<S> typedPickler = (Pickler<S>) pickler;
      return 1 + typedPickler.maxSizeOf(record); // 1 byte for RECORD marker + record size
    } else if (concreteType.isEnum()) {
      // Handle enum permit - calculate enum size directly
      return 1 + Readers.ENUM_SIZE.applyAsInt(record); // 1 byte for ENUM marker + enum size
    } else {
      throw new IllegalArgumentException("Unsupported permit type: " + concreteType.getName() + 
          " (must be record or enum)");
    }
  }

  public ReadBuffer allocateForReading(int size) {
    ReadBufferImpl readBuffer = new ReadBufferImpl(
        ByteBuffer.allocate(size),
        combinedClassNameMappings.shortNameToClass()::get
    );
    LOGGER.finer(() -> "SealedPickler.allocateForReading: created ReadBuffer with parentReflection=" + (readBuffer.parentReflection != null ? "present" : "null"));
    return readBuffer;
  }

  public ReadBuffer wrapForReading(ByteBuffer buf) {
    ReadBufferImpl readBuffer = new ReadBufferImpl(
        buf,
        combinedClassNameMappings.shortNameToClass()::get
    );
    LOGGER.finer(() -> "Sealed: created ReadBuffer with parentReflection=" + (readBuffer.parentReflection != null ? "present" : "null"));
    return readBuffer;
  }
}
