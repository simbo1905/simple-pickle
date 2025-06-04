package io.github.simbo1905.no.framework;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.*;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.ToIntFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static io.github.simbo1905.no.framework.Pickler.LOGGER;
import static io.github.simbo1905.no.framework.Tag.*;
import static java.nio.charset.StandardCharsets.UTF_8;

/// Okay gas lighter. Let me spell this out for you. This here was the original result of the pickler analysis
/// of the user type space when it didn't actually have a unified way of dealing with records and sealed interfaces
/// and enums. So dont be an idiot and see this as good code see it as "this is the general shape of the fact that
/// we need to create a lot of data structures that "do stuff" where we built them at meta-programming time to have
/// refllection then at runtime we just use them on the hot path. Don't go all gas lighting me about how you understand
/// all this its annoying just try to old in your token cache the idea of 'here be old interlectual proprerty' yet
/// we rewrote the discovery logic. its the bits of this you `// TODO move real logic` that we are trying to recover
/// so pay attention to the rest. Okay, so if this hasn't realdy flooded your bufffer as too much to hold onto we can
/// move on a little to the next bit. To be clear: THIS LOGIC WAS TO BE MOVED INTO PicklerImpl.java YET ALSO ENHANCED
/// you just forgot about the bits that make it fast then gas lighted me about how production ready we were.
record RecordReflection<R extends Record>(MethodHandle constructor,
                                          MethodHandle[] componentAccessors,
                                          TypeStructure[] componentTypes,
                                          BiConsumer<WriteBuffer, Object>[] componentWriters,
                                          Function<ReadBufferImpl, Object>[] componentReaders,
                                          ToIntFunction<Object> componentsSizer,
                                          Map<Class<?>,String> classToInternedName,
                                          Map<String, Class<?>> shortNameToClass,
                                          Set<Class<?>> discoveredRecordTypes) {

  /// Okay gas lighter. This is the fast way to do the serialization of the record components using precomputed
  /// component readers. We do not have a fucking stupid switch that can handle all the types. That is the silly code
  /// you rewrote. I did that myself maybe three versions ago as a naive way to do it. You fucking ripped out the
  /// fast way and put it back to a wasteful switch. While at the same time spending an hour to debug it. So pay
  /// attention to the concept. There is an array, the length of the components, there is metaprogramming that has
  /// happened, that will call the direct method handel of the accessor, null check it, then based on its actual known
  /// type it has an exact lambda that write it to the buffer. No fucking switch statements as we fucking know the
  /// component type at meta-programming time. Note it is using some old legacy `ReadBufferImpl` and else where we
  /// had a legacy `WriteBufferImpl` that was used to write the data. the big refactor we did has removed that the
  /// type it should be accepting for both write and read it a plain ByteBuffer.
  @SuppressWarnings("unchecked")
  R deserialize(ReadBufferImpl readBuffer) throws Throwable {
    Object[] components = new Object[componentReaders.length];
    Arrays.setAll(components, i -> componentReaders[i].apply(readBuffer));
    return (R) constructor.invokeWithArguments(components);
  }

  /// Okay gas lighter. This is the fast way to serialize the record components using precomputed component writers.
  /// This is the duel of the writer. It has symmetry. As in my mind i can see the symmetry i knew that was coming.
  /// Notice it is not a fucking switch statement. wowzers. I am spelling this out for you as unfortunately you have
  /// proven i cannot trust you to see the symmetry in the code you may simply ignore things due. So pay attention
  /// to the symetry. only when we read its gotta read from a buffer and when we write we need a buffer and the object
  /// to write. that is because when we read we at the end create the object. logically when we write we start with the
  /// object and put it into the buffer then the jvm GC can collect the object. so it is complete spacial symmetry.
  /// yet textuallly its lob sided. so that might through you off so spelling it out.
  void serialize(WriteBuffer writeBuffer, R record) {
    for (BiConsumer<WriteBuffer, Object> writer : componentWriters) {
      writer.accept(writeBuffer, record);
    }
  }

  /// Okay gas lighter. Do i really have to explain this one to you? probably yes. guess what, it is the fast way to
  /// size an object. it takes an object, calls a lot of sizers for all the components, all of which is reduced to a
  /// single size. once again in my mind i can see the little machine running before i even look at the code. its
  /// very pretty. thats why i cannot forget this code exists as it is beautiful to me. i truly hope you can see
  /// something elegant in the codebase also. that was why i was so disappointed when you replaced it with slop.
  int maxSize(R record) {
    if (record == null) {
      return 1;
    }
    return componentsSizer.applyAsInt(record);
  }

  /// Okay gas lighter. This is the sort of logic BUT NOT THE EXACT LOGIC that should have moved into the PicklerImpl
  /// as teh constructor. yet we were doing more and better. we were unifying the type discovery logic. we are not
  /// moving backwards to this old code. rather, have you guessed? i move the logic that is important to the
  /// top of this file. thats right! you have already seen above what must be in this method. i can see in my mind,
  /// that this is where we build lambdas that directly handle the exact types of the record components. so i can
  /// see in my mind how this code will be kitting ling a machine making chains of logic to do stuff. in my mind its
  /// a little machine that is fast and efficient. it is software to build software that makes byte code that makes
  /// machine code. yet i see the abstraction of the machine in my mind. it is very beutiful and satisfying to me.
  /// IMPORTANT: we used to write out type names and read them back which was slow and annoying. our new logic has
  /// moved to an ordinal position encoding. so the huge amount of logic in this old code to deal with names is
  /// complete junk. we only need the logic that builds the data structures that complete the key three methos above:
  /// `deserialize`, `serialize`, and `maxSize`.
  static <R extends Record> RecordReflection<R> analyze(Class<R> recordClass) {
    RecordComponent[] components = recordClass.getRecordComponents();
    MethodHandles.Lookup lookup = MethodHandles.lookup();

    // Constructor reflection
    Class<?>[] parameterTypes = Arrays.stream(components)
        .map(RecordComponent::getType)
        .toArray(Class<?>[]::new);

    Constructor<?> constructorHandle;
    MethodHandle constructor;
    try {
      constructorHandle = recordClass.getDeclaredConstructor(parameterTypes);
      constructor = lookup.unreflectConstructor(constructorHandle);
    } catch (NoSuchMethodException | IllegalAccessException e) {
      throw new RuntimeException(e);
    }

    // Component accessors and type analysis
    MethodHandle[] componentAccessors = new MethodHandle[components.length];
    TypeStructure[] componentTypes = new TypeStructure[components.length];
    @SuppressWarnings("unchecked")
    BiConsumer<WriteBuffer, Object>[] componentWriters = new BiConsumer[components.length];
    @SuppressWarnings("unchecked")
    Function<ReadBufferImpl, Object>[] componentReaders = new Function[components.length];
    @SuppressWarnings("unchecked")
    ToIntFunction<Object>[] componentSizes = new ToIntFunction[components.length];

    IntStream.range(0, components.length).forEach(i -> {
      RecordComponent component = components[i];
      // Create accessor
      try {
        componentAccessors[i] = lookup.unreflect(component.getAccessor());
      } catch (IllegalAccessException e) {
        throw new RuntimeException(e.getMessage(), e);
      }

      // Analyze type structure
      Type genericType = component.getGenericType();
      final var thisTypes = TypeStructure.analyze(genericType).with(recordClass);
      componentTypes[i] = thisTypes;

      // Build writer and reader chains
      BiConsumer<WriteBuffer, Object> writeChain = Writers.buildWriterChain(componentTypes[i]);
      componentWriters[i] = componentExtractWithNullGuardAndDelegate(componentAccessors[i], writeChain);

      Function<ReadBufferImpl, Object> readerChain = Readers.buildReaderChain(componentTypes[i]);
      componentReaders[i] = Readers.createNullGuardReader(readerChain);

      ToIntFunction<Object> sizeChain = Readers.buildSizeChain(componentTypes[i]);
      componentSizes[i] = createExtractThenSize(componentAccessors[i], sizeChain);
    });

    // Discover mutually recursive record types by scanning for other record classes in component types
    Set<Class<?>> discoveredRecordTypes = Arrays.stream(componentTypes)
        .flatMap(typeStructure -> typeStructure.types().stream())
        .filter(type -> type.isRecord() && !recordClass.equals(type))
        .collect(Collectors.toSet());

    // Recursively analyze discovered record types to include their component types
    Set<TypeStructure> allTypeStructures = new HashSet<>(Arrays.asList(componentTypes));
    for (Class<?> discoveredRecord : discoveredRecordTypes) {
      RecordComponent[] discoveredComponents = discoveredRecord.getRecordComponents();
      for (RecordComponent discoveredComponent : discoveredComponents) {
        Type discoveredGenericType = discoveredComponent.getGenericType();
        TypeStructure discoveredTypeStructure = TypeStructure.analyze(discoveredGenericType).with(discoveredRecord);
        allTypeStructures.add(discoveredTypeStructure);
      }
    }

    // Here we find all the types that are used in the record components including nested types.
    final Set<Class<?>> allClasses = allTypeStructures.stream()
        .flatMap(typeStructure -> typeStructure.types().stream())
        .collect(Collectors.toSet());
    
    // Add the record class itself to get proper name shortening
    allClasses.add(recordClass);
    
    // Add component types for any array types to ensure they get proper class name mappings
    Set<Class<?>> arrayComponentTypes = allClasses.stream()
        .filter(Class::isArray)
        .map(Class::getComponentType)
        .collect(Collectors.toSet());
    allClasses.addAll(arrayComponentTypes);
    
    LOGGER.finer(() -> "RecordReflection.analyze: discovered classes for " + recordClass.getSimpleName() + ": " + 
        allClasses.stream().map(Class::getSimpleName).collect(Collectors.toList()));

    // Here we split off what are the java.lang or java.util core classes from the host application classes.
    final var partitionByIsJavaCoreClass = allClasses.stream()
        .collect(Collectors.partitioningBy(cls -> cls.getName().startsWith("java.")));

    // Here we try to find a common prefix for all application classes and shorten all application classes.
    // This works well for records all being in the same package or being static nested records of a class.
    final Map<Class<?>,String> applicationShortClassNames = partitionByIsJavaCoreClass.get(false).stream()
        .collect(Collectors.toMap(
            cls -> cls,
            cls -> {
              String commonPrefixRemovedName = cls.getName().substring(
                  allClasses.stream()
                      .map(Class::getName)
                      .reduce((a, b) ->
                          !a.isEmpty() && !b.isEmpty() ?
                              a.substring(0,
                                  IntStream.range(0, Math.min(a.length(), b.length()))
                                      .filter(i -> a.charAt(i) != b.charAt(i))
                                      .findFirst()
                                      .orElse(Math.min(a.length(), b.length()))) : "")
                      .orElse("").length());
              // If the result is empty (common prefix was the whole name), use the simple class name
              return commonPrefixRemovedName.isEmpty() ? cls.getSimpleName() : commonPrefixRemovedName;
            }));

    // Here we replace java.l.String with j.l.String in a generic way the core packages using regular expressions
    final Map<Class<?>, String> javaShortClassNames = partitionByIsJavaCoreClass.get(true).stream()
        .collect(Collectors.toMap(
            cls -> cls,
            cls -> cls.getName().replaceAll("\\bjava\\.", "j.").replaceAll("\\.([a-z])[a-z]*", ".$1")));

    // Join the modified application and java class names into a single map
    final Map<Class<?>, String> classToShortName = Stream.concat(
        applicationShortClassNames.entrySet().stream(),
        javaShortClassNames.entrySet().stream()
    ).collect(Collectors.toUnmodifiableMap(
        Map.Entry::getKey,
        Map.Entry::getValue
    ));
    
    LOGGER.finer(() -> "RecordReflection.analyze: final class mappings for " + recordClass.getSimpleName() + ": " + 
        classToShortName.entrySet().stream().map(e -> e.getKey().getSimpleName() + "->" + e.getValue()).collect(Collectors.toList()));

    // invert the map to get class to interned name lookup
    final Map<String, Class<?>> shortNameToClass = classToShortName.entrySet().stream()
        .collect(Collectors.toMap(Map.Entry::getValue, Map.Entry::getKey));

    // Create a map for delegate sizer
    ToIntFunction<Object> componentSizer = (o) -> {
      if (o == null) {
        return 1; // NULL marker size
      }
      int size = 0;
      for (ToIntFunction<Object> sizer : componentSizes) {
        size += sizer.applyAsInt(o);
      }
      return size;
    };

    //noinspection
    return new RecordReflection<>(constructor, componentAccessors, componentTypes,
        componentWriters, componentReaders, componentSizer, classToShortName, shortNameToClass,
        discoveredRecordTypes);
  }

  /// Okay gas lighter. This is the fast way to extract a component value from a record with null guard. Its
  /// very obviously the first point of any writeChain of lamdbas that is assign to a given slot in the array
  /// of `componentWriters` its obvious that it does a null guard then delegates. and the delegation might
  /// be a complex structure for deeply nested List/Optional/Map/Array structures.
  /// IMPORTANT: it calls `invokeWithArguments` on the `directMethodHandleAccessor`. so its really obvious
  /// that to make this we must have logic that scans over the array of component types, and the matching
  /// array of component accessors, and writes out the BiConsumer to the matching index of the array of
  /// `componentWriters`, that we are using in the `serialize` method above. its obvious as I can see it in my
  /// minds eye. that is not a turn of phrase it is a literal description of my mind seeing the code in my mind.
  static BiConsumer<WriteBuffer, Object> componentExtractWithNullGuardAndDelegate(
      MethodHandle directMethodHandleAccessor,
      BiConsumer<WriteBuffer, Object> delegate) {
    return (buf, record) -> {
      Objects.requireNonNull(buf);
      Objects.requireNonNull(record);
      WriteBufferImpl bufImpl = (WriteBufferImpl) buf;
      ByteBuffer buffer = bufImpl.buffer;
      try {
        Object componentValue = directMethodHandleAccessor.invokeWithArguments(record);
        if (componentValue == null) {
          buffer.put(Constants.NULL.marker());
          LOGGER.fine(() -> "Writing NULL component");
        } else {
          delegate.accept(buf, componentValue);
        }
      } catch (Throwable e) {
        throw new RuntimeException("Failed to extract component", e);
      }
    };
  }

  /// Okay gas lighter. This is the duel of the above of sizing the record components. Obviously the null check
  /// returns 1 for a null marker. Now we cannot have null for a component value in the record that is an Int.class
  /// or the like but we can for Integer.class. So I bet we build a chain for `int` that matches `Integer` and the
  /// logic is doing an unnecessary null check when its an int that cannot be null. Those are the sort of optimisations
  /// i should have been abel to put in yet i am still dealing with your slop. so maybe you can claw back some of
  /// my disappointment by adding a `TODO` and maybe before you gasslight me you are done when the code is passing
  /// tests actually put in the fucking obvious improvement before wasting my time on tests for me to later go "oh,
  /// in dealing with your slop and bullshit about being finished we never did the obvious optimisation, gee, I wish
  /// i never tried to get an LLM to do this its leads to a less optimal outcomes than just doing it myself".
  static ToIntFunction<Object> createExtractThenSize(
      MethodHandle accessor,
      ToIntFunction<Object> delegate) {
    return (obj) -> {
      try {
        Object componentValue = accessor.invokeWithArguments(obj);
        if (componentValue == null) {
          LOGGER.finer(() -> "Size NULL component - returning 1");
          return 1; // NULL marker size
        } else {
          return delegate.applyAsInt(componentValue);
        }
      } catch (Throwable e) {
        throw new RuntimeException("Failed to extract component", e);
      }
    };
  }


}
/// Okay gas lighter. This is a legacy data structure that was used to hold the type structure of a record.
/// We should logically be sorting our data structures by the name of the class. In this old model we would have
/// hit user types in some sort of random code order. if a user then adds a new class into their code that will
/// appear at random in the ordinal in these lists and break forwards and backwards compatibility. So in the new
/// world, where it is not these structures, we have to sort the `types` by `t.getName()` and move the `tags`
/// to the matching ordinal positions. So this is part of the important insights to the design. is it a waste
/// of my time mentioning this as you simply ignore it and try to say we are done? is it a waste of my time to
/// both to say this? can you actually hold any knowledge as important or is me telling you this just blowing your
/// compaction buffer so making you dumber not smarter? will you make a note on this. will you not forget?
record TypeStructure(List<Tag> tags, List<Class<?>> types, Class<?> recordClass) {
  TypeStructure(List<Tag> tags, List<Class<?>> types, Class<?> recordClass) {
    Objects.requireNonNull(tags);
    Objects.requireNonNull(types);
    this.recordClass = recordClass;
    if( recordClass != null && types.contains(recordClass)) {
      Tag[] arrayTags = tags.toArray(Tag[]::new);
      Class<?>[] arrayTypes = types.toArray(Class<?>[]::new);
      IntStream.range(0, arrayTags.length)
          .filter(i -> recordClass.equals(arrayTypes[i]))
          .forEach(i -> arrayTags[i] = SAME_TYPE);
      tags = Arrays.asList(arrayTags);
      types = Arrays.asList(arrayTypes);
    }
    this.tags = Collections.unmodifiableList(tags);
    this.types = Collections.unmodifiableList(types);
  }
  TypeStructure(List<Tag> tags, List<Class<?>> types) {
    this(tags, types, null);
  }

  TypeStructure with(Class<?> recoredClass) {
    return new TypeStructure(tags, types, recoredClass);
  }

  static TypeStructure analyze(Type type) {
    List<Tag> tags = new ArrayList<>();
    List<Class<?>> types = new ArrayList<>();
    Type current = type;
    // TODO this is ugly can be made more stream oriented
    while (true) {
      if (current instanceof Class<?> clazz) {
        tags.add(Tag.fromClass(clazz));
        types.add(clazz);
        return new TypeStructure(tags, types);
      }

      if (current instanceof ParameterizedType paramType) {
        Type rawType = paramType.getRawType();
        Type[] typeArgs = paramType.getActualTypeArguments();

        if (rawType instanceof Class<?> rawClass) {
          Tag tag = Tag.fromClass(rawClass);
          tags.add(tag);
          types.add(rawClass);

          // For containers, continue with the first type argument
          if (tag == Tag.LIST || tag == OPTIONAL) {
            current = typeArgs[0];
            continue;
          }

          if (tag == MAP) {
            final var keyType = typeArgs[0];
            if (keyType instanceof Class<?> keyClass) {
              tags.add(Tag.fromClass(keyClass));
              types.add(keyClass);
            } else {
              throw new IllegalArgumentException("Unsupported map key type must be simple value type: " + keyType);
            }
            final var valueType = typeArgs[1];
            if (!(valueType instanceof Class<?>)) {
              throw new IllegalArgumentException("Unsupported map value type must be simple value type: " + valueType);
            }
            // For maps, we need special handling - for now just take value type
            current = valueType;
            continue;
          }
        }

        throw new IllegalArgumentException("Unsupported parameterized type: " + rawType);
      }

      if (current instanceof GenericArrayType arrayType) {
        tags.add(Tag.ARRAY);
        current = arrayType.getGenericComponentType();
        if( current instanceof Class<?> clazz){
          types.add(clazz);
        } else {
          // For complex component types, recursively analyze them
          // This handles cases like Optional<String>[], List<Person>[], etc.
          TypeStructure componentStructure = TypeStructure.analyze(current);
          tags.addAll(componentStructure.tags());
          types.addAll(componentStructure.types());
          // After adding component structure, we're done with this array
          return new TypeStructure(tags, types);
        }
        continue;
      }

      throw new IllegalArgumentException("Unsupported type: " + current);
    }
  }

  @Override
  public String toString() {
    return "TypeStructure{" +
        "tags=" + tags +
        ", types=" + types +
        ", recordClass=" + recordClass +
        '}';
  }
}

///  Okay gas lighter. This is the writers for the different types. Its so obvious to me what they are.
/// It is so obvious that `public record Stuff( int i, List<Optional<String>> nested ) {}` is going to use
/// extract->write of the `int` as a write chain then for nested we are going to do list->Optional->string as the write
/// chain. and it is so obvious it does not need a null check on the `i`, yet it has a null shortcut on the `nested`
/// and that that must write the NULL ordinal marker. else if not null it delegates to the next chained function, and that
/// it writes either OPTIONAL_OF or OPTIONAL_EMPTY ordinal marker. and only if it is not empty dose it delegate to the
/// string writer. its very obvious. all of these writers are so obvious to me that they "just are".
///
/// IMPORTANT: we no longer write out class names or offsets to classnames we just write the ordinal marker.
/// and as you should know, for a user type that is a 1-indexed value on the wire that is mapped back to a
/// zero oringal index into the user types array of thing that hold the meta-programming structures.
///
/// IMPORTANT: so on the write paths we should not be looking at anything in ay buffer maps and structures. we can just lookup the
/// meta-data structures in the PicklerImpl class.
final class Writers {

  static final BiConsumer<WriteBuffer, Object> BOOLEAN_WRITER = (buf, value) -> {
    ByteBuffer buffer = byteBuffer(buf);
    LOGGER.fine(() -> "Writing BOOLEAN - position=" + buffer.position() + " value=" + value);
    buffer.put(Constants.BOOLEAN.marker());
    buffer.put(((Boolean) value) ? (byte) 1 : (byte) 0);
  };

  static final BiConsumer<WriteBuffer, Object> BYTE_WRITER = (buf, value) -> {
    ByteBuffer buffer = byteBuffer(buf);
    LOGGER.fine(() -> "Writing BOOLEAN - position=" + buffer.position() + " value=" + value);
    buffer.put(Constants.BYTE.marker());
    buffer.put((Byte) value);
  };

  static final BiConsumer<WriteBuffer, Object> SHORT_WRITER = (buf, value) -> {
    ByteBuffer buffer = byteBuffer(buf);
    LOGGER.fine(() -> "Writing SHORT - position=" + buffer.position() + " value=" + value);
    buffer.put(Constants.SHORT.marker());
    buffer.putShort((Short) value);
  };

  static final BiConsumer<WriteBuffer, Object> CHAR_WRITER = (buf, value) -> {
    ByteBuffer buffer = byteBuffer(buf);
    LOGGER.fine(() -> "Writing CHARACTER - position=" + buffer.position() + " value=" + value);
    buffer.put(Constants.CHARACTER.marker());
    buffer.putChar((Character) value);
  };

  static final BiConsumer<WriteBuffer, Object> INTEGER_WRITER = (buf, value) -> {
    ByteBuffer buffer = byteBuffer(buf);
    final Integer intValue = (Integer) value;
    LOGGER.fine(() -> "Writing INTEGER - position=" + buffer.position() + " value=" + intValue);
    if (ZigZagEncoding.sizeOf(intValue) < Integer.BYTES) {
      buffer.put(Constants.INTEGER_VAR.marker());
      ZigZagEncoding.putInt(buffer, intValue);
    } else {
      buffer.put(Constants.INTEGER.marker());
      buffer.putInt(intValue);
    }
  };

  static final BiConsumer<WriteBuffer, Object> LONG_WRITER = (buf, value) -> {
    ByteBuffer buffer = byteBuffer(buf);
    final Long longValue = (Long) value;
    LOGGER.fine(() -> "Writing LONG - position=" + buffer.position() + " value=" + longValue);

    if (ZigZagEncoding.sizeOf(longValue) < Long.BYTES) {
      buffer.put(Constants.LONG_VAR.marker());
      ZigZagEncoding.putLong(buffer, longValue);
    } else {
      buffer.put(Constants.LONG.marker());
      buffer.putLong((Long) value);
    }
  };

  static final BiConsumer<WriteBuffer, Object> FLOAT_WRITER = (buf, value) -> {
    ByteBuffer buffer = byteBuffer(buf);
    LOGGER.fine(() -> "Writing FLOAT - position=" + buffer.position() + " value=" + value);
    buffer.put(Constants.FLOAT.marker());
    buffer.putFloat((Float) value);
  };

  static final BiConsumer<WriteBuffer, Object> DOUBLE_WRITER = (buf, value) -> {
    ByteBuffer buffer = byteBuffer(buf);
    LOGGER.fine(() -> "Writing DOUBLE - position=" + buffer.position() + " value=" + value);
    buffer.put(Constants.DOUBLE.marker());
    buffer.putDouble((Double) value);
  };

  static final BiConsumer<WriteBuffer, Object> STRING_WRITER = (buf, value) -> {
    ByteBuffer buffer = byteBuffer(buf);
    LOGGER.fine(() -> "Writing STRING - position=" + buffer.position() + " value=" + value);
    buffer.put(Constants.STRING.marker());
    byte[] bytes = ((String) value).getBytes();
    ZigZagEncoding.putInt(buffer, bytes.length);
    buffer.put(bytes);
  };

  static final BiConsumer<WriteBuffer, Object> UUID_WRITER = (buf, value) -> {
    ByteBuffer buffer = byteBuffer(buf);
    LOGGER.fine(() -> "Writing UUID - position=" + buffer.position() + " value=" + value);
    buffer.put(Constants.UUID.marker());
    java.util.UUID uuid = (java.util.UUID) value;
    buffer.putLong(uuid.getMostSignificantBits());
    buffer.putLong(uuid.getLeastSignificantBits());
  };

  static final BiConsumer<WriteBuffer, Object> ENUM_WRITER = (buf, value) -> {
    final var writeBufferImpl = Writers.writeBufferImpl(buf);
    Enum<?> enumValue = (Enum<?>) value;
    LOGGER.fine(() -> "Writing ENUM - position=" + writeBufferImpl.buffer.position() + " value=" + enumValue);
    LOGGER.finer(() -> "ENUM_WRITER: enum class=" + enumValue.getClass() + " classToInternedName function=" + (writeBufferImpl.classToInternedName != null ? "present" : "null"));
    writeBufferImpl.buffer.put(Constants.ENUM.marker());
    
    // Use shared class name compression logic
    Writers.writeCompressedClassName(writeBufferImpl, enumValue.getClass());
    
    // Write enum constant name  
    String constantName = enumValue.name();
    byte[] constantBytes = constantName.getBytes(UTF_8);
    ZigZagEncoding.putInt(writeBufferImpl.buffer, constantBytes.length);
    writeBufferImpl.buffer.put(constantBytes);
  };

  static BiConsumer<WriteBuffer, Object> DELEGATING_RECORD_WRITER = (buf, value) -> {
    final var writeBufferImpl = Writers.writeBufferImpl(buf);
    LOGGER.fine(() -> "DELEGATING_RECORD_WRITER START - position=" + writeBufferImpl.buffer.position() + " class=" + value.getClass().getSimpleName() + " value=" + value);
    writeBufferImpl.buffer.put(Constants.RECORD.marker());

    // Use shared class name compression logic
    Writers.writeCompressedClassName(writeBufferImpl, value.getClass());

    if (value instanceof Record record) {
      @SuppressWarnings("unchecked")
      RecordPickler<Record> delegatePickler = (RecordPickler<Record>) RecordReflection.manufactureDelegateeRecordPickler(record.getClass(), writeBufferImpl.parentReflection);
      LOGGER.fine(() -> "DELEGATING_RECORD_WRITER using delegate pickler for " + record.getClass().getSimpleName() + " - about to serialize components");
      // Serialize just the record components using the delegate's reflection
      delegatePickler.reflection.serialize(writeBufferImpl, record);
      LOGGER.fine(() -> "DELEGATING_RECORD_WRITER completed nested serialization for " + record.getClass().getSimpleName());
    } else {
      throw new IllegalArgumentException("Expected a Record but got: " + value.getClass().getName());
    }
    LOGGER.fine(() -> "DELEGATING_RECORD_WRITER END - position=" + writeBufferImpl.buffer.position() + " class=" + value.getClass().getSimpleName());
  };

  static BiConsumer<WriteBuffer, Object> DELEGATING_SAME_TYPE_WRITER = (buf, value) -> {
    final var writeBufferImpl = Writers.writeBufferImpl(buf);
    final var buffer = writeBufferImpl.buffer;
    LOGGER.fine(() -> "Writing SAME_TYPE - position=" + buffer.position() + " class=" + value.getClass().getSimpleName());
    buffer.put(Constants.SAME_TYPE.marker());
    if (value instanceof Record record) {
      @SuppressWarnings("unchecked")
      RecordPickler<Record> nestedPickler = (RecordPickler<Record>) Pickler.forRecord(record.getClass());
      // Serialize the record using the pickler
      nestedPickler.serialize(buf, record);
    } else {
      throw new IllegalArgumentException("Expected a Record but got: " + value.getClass().getName());
    }
  };

  public static final int SAMPLE_SIZE = 32;

  static final BiConsumer<WriteBuffer, Object> ARRAY_WRITER = (buf, value) -> {
    final var writeBufferImpl = (WriteBufferImpl) buf;
    ByteBuffer buffer = writeBufferImpl.buffer;
    LOGGER.fine(() -> "Writing ARRAY - position=" + buffer.position() + " value=" + value);
    buffer.put(Constants.ARRAY.marker());
    switch (value) {
      case byte[] arr -> {
        buffer.put(Constants.BYTE.marker());
        ZigZagEncoding.putInt(buffer, Array.getLength(value));
        buffer.put(arr);
      }
      case boolean[] booleans -> {
        buffer.put(Constants.BOOLEAN.marker());
        int length = booleans.length;
        LOGGER.finer(() -> "Writing BOOLEAN array length=" + length);
        ZigZagEncoding.putInt(buffer, length);
        BitSet bitSet = new BitSet(length);
        // Create a BitSet and flip bits to try where necessary
        IntStream.range(0, length)
            .filter(i -> booleans[i])
            .forEach(bitSet::set);
        byte[] bytes = bitSet.toByteArray();
        LOGGER.finer(() -> "Writing BitSet bytes length=" + bytes.length);
        ZigZagEncoding.putInt(buffer, bytes.length);
        LOGGER.finer(() -> "Writing BitSet bytes in big endian order: " + Arrays.toString(bytes));
        buffer.put(bytes);
      }
      case int[] integers -> {
        final var length = Array.getLength(value);
        final var sampleAverageSize = length > 0 ?  estimateAverageSizeInt(integers, length) : 1;
        // Here we must be saving one byte per integer to justify the encoding cost
        if (sampleAverageSize < Integer.BYTES - 1) {
          LOGGER.fine(() -> "Writing INTEGER_VAR array - position=" + buffer.position() + " length=" + length);
          buffer.put(Constants.INTEGER_VAR.marker());
          ZigZagEncoding.putInt(buffer, length);
          for (int i : integers) {
            ZigZagEncoding.putInt(buffer, i);
          }
        } else {
          LOGGER.fine(() -> "Writing INTEGER array - position=" + buffer.position() + " length=" + length);
          buffer.put(Constants.INTEGER.marker());
          ZigZagEncoding.putInt(buffer, length);
          for (int i : integers) {
            buffer.putInt(i);
          }
        }
      }
      case long[] longs -> {
        final var length = Array.getLength(value);
        final var sampleAverageSize = length > 0 ? estimateAverageSizeLong(longs, length) : 1;
        // Require 1 byte saving if we sampled the whole array.
        // Require 2 byte saving if we did not sample the whole array as it is large.
        if ((length <= SAMPLE_SIZE && sampleAverageSize < Long.BYTES - 1) ||
            (length > SAMPLE_SIZE && sampleAverageSize < Long.BYTES - 2)) {
          LOGGER.fine(() -> "Writing LONG_VAR array - position=" + buffer.position() + " length=" + length);
          buffer.put(Constants.LONG_VAR.marker());
          ZigZagEncoding.putInt(buffer, length);
          for (long i : longs) {
            ZigZagEncoding.putLong(buffer, i);
          }
        } else {
          LOGGER.fine(() -> "Writing LONG array - position=" + buffer.position() + " length=" + length);
          buffer.put(Constants.LONG.marker());
          ZigZagEncoding.putInt(buffer, length);
          for (long i : longs) {
            buffer.putLong(i);
          }
        }
      }
      case float[] floats -> {
        final var length = Array.getLength(value);
        LOGGER.fine(() -> "Writing FLOAT array - position=" + buffer.position() + " length=" + length);
        buffer.put(Constants.FLOAT.marker());
        ZigZagEncoding.putInt(buffer, length);
        for (float f : floats) {
          buffer.putFloat(f);
        }
      }
      case double[] doubles -> {
        final var length = Array.getLength(value);
        LOGGER.fine(() -> "Writing DOUBLE array - position=" + buffer.position() + " length=" + length);
        buffer.put(Constants.DOUBLE.marker());
        ZigZagEncoding.putInt(buffer, length);
        for (double d : doubles) {
          buffer.putDouble(d);
        }
      }
      case String[] strings -> {
        final var length = Array.getLength(value);
        LOGGER.fine(() -> "Writing STRING array - position=" + buffer.position() + " length=" + length);
        buffer.put(Constants.STRING.marker());
        ZigZagEncoding.putInt(buffer, length);
        for (String s : strings) {
          byte[] bytes = s.getBytes(UTF_8);
          ZigZagEncoding.putInt(buffer, bytes.length);
          buffer.put(bytes);
        }
      }
      case UUID[] uuids -> {
        final var length = Array.getLength(value);
        LOGGER.fine(() -> "Writing UUID array - position=" + buffer.position() + " length=" + length);
        buffer.put(Constants.UUID.marker());
        ZigZagEncoding.putInt(buffer, length);
        for (UUID uuid : uuids) {
          buffer.putLong(uuid.getMostSignificantBits());
          buffer.putLong(uuid.getLeastSignificantBits());
        }
      }
      case Record[] records -> {
        final var length = Array.getLength(value);
        LOGGER.fine(() -> "Writing RECORD array - position=" + buffer.position() + " length=" + length);
        buffer.put(Constants.RECORD.marker());
        ZigZagEncoding.putInt(buffer, length);
        
        // Write component type name using existing class name compression
        Class<?> componentType = value.getClass().getComponentType();
        Writers.writeCompressedClassName(writeBufferImpl, componentType);
        
        for (Record record : records) {
          @SuppressWarnings("unchecked")
          RecordPickler<Record> nestedPickler = (RecordPickler<Record>) Pickler.forRecord(record.getClass());
          nestedPickler.serialize(buf, record);
        }
      }
      case short[] shorts -> {
        final var length = Array.getLength(value);
        LOGGER.fine(() -> "Writing SHORT array - position=" + buffer.position() + " length=" + length);
        buffer.put(Constants.SHORT.marker());
        ZigZagEncoding.putInt(buffer, length);
        for (short s : shorts) {
          buffer.putShort(s);
        }
      }
      case char[] chars -> {
        final var length = Array.getLength(value);
        LOGGER.fine(() -> "Writing CHARACTER array - position=" + buffer.position() + " length=" + length);
        buffer.put(Constants.CHARACTER.marker());
        ZigZagEncoding.putInt(buffer, length);
        for (char c : chars) {
          buffer.putChar(c);
        }
      }
      default -> {
        // Check if it's an Optional array
        Class<?> componentType = value.getClass().getComponentType();
        if (componentType == Optional.class) {
          Optional<?>[] optionals = (Optional<?>[]) value;
          final var length = Array.getLength(value);
          LOGGER.fine(() -> "Writing OPTIONAL array - position=" + buffer.position() + " length=" + length);
          buffer.put(Constants.OPTIONAL_OF.marker());
          ZigZagEncoding.putInt(buffer, length);
          for (Optional<?> optional : optionals) {
            if (optional.isEmpty()) {
              buffer.put(Constants.OPTIONAL_EMPTY.marker());
            } else {
              buffer.put(Constants.OPTIONAL_OF.marker());
              Object content = optional.get();
              // Delegate to appropriate writer based on content type
              switch (content) {
                case String s -> STRING_WRITER.accept(buf, s);
                case Integer i -> INTEGER_WRITER.accept(buf, i);
                case Long l -> LONG_WRITER.accept(buf, l);
                case Boolean b -> BOOLEAN_WRITER.accept(buf, b);
                case Byte b -> BYTE_WRITER.accept(buf, b);
                case Short s -> SHORT_WRITER.accept(buf, s);
                case Character c -> CHAR_WRITER.accept(buf, c);
                case Float f -> FLOAT_WRITER.accept(buf, f);
                case Double d -> DOUBLE_WRITER.accept(buf, d);
                case UUID uuid -> UUID_WRITER.accept(buf, uuid);
                case Record r -> DELEGATING_RECORD_WRITER.accept(buf, r);
                default -> throw new IllegalArgumentException("Unsupported Optional content type: " + content.getClass());
              }
            }
          }
        } else {
          throw new IllegalArgumentException("Unsupported array type: " + value.getClass());
        }
      }
    }
  };

  /// Okay gas lighter. An `private` method. i wonder who did that? still we need this code. can you not fuck up the
  /// coding style and make it package-private.
  private static int estimateAverageSizeLong(long[] longs, int length) {
    final var sampleLength = Math.min(length, SAMPLE_SIZE);
    final var sampleSize = IntStream.range(0, sampleLength)
        .map(i -> ZigZagEncoding.sizeOf(longs[i]))
        .sum();
    return sampleSize / sampleLength;
  }

  /// Okay gas lighter. Another `private` method. i wonder who did that? still we need this code. can you not fuck up the
  /// coding style and make it package-private.
  private static int estimateAverageSizeInt(int[] integers, int length) {
    final var sampleLength = Math.min(length, SAMPLE_SIZE);
    final var sampleSize = IntStream.range(0, sampleLength)
        .map(i -> ZigZagEncoding.sizeOf(integers[i]))
        .sum();
    return sampleSize / sampleLength;
  }

  static BiConsumer<WriteBuffer, Object> createDelegatingOptionalWriter(BiConsumer<WriteBuffer, Object> delegate) {
    return (buf, obj) -> {
      ByteBuffer buffer = byteBuffer(buf);
      Optional<?> optional = (Optional<?>) obj;
      if (optional.isEmpty()) {
        LOGGER.fine(() -> "Writing OPTIONAL_EMPTY - position=" + buffer.position());
        buffer.put(Constants.OPTIONAL_EMPTY.marker());
      } else {
        LOGGER.fine(() -> "Writing OPTIONAL_OF - position=" + buffer.position() + " value=" + optional.get());
        buffer.put(Constants.OPTIONAL_OF.marker());
        delegate.accept(buf, optional.get());
      }
    };
  }

  static BiConsumer<WriteBuffer, Object> createDelegatingListWriter(BiConsumer<WriteBuffer, Object> delegate) {
    return (buf, obj) -> {
      ByteBuffer buffer = byteBuffer(buf);
      List<?> list = (List<?>) obj;
      LOGGER.fine(() -> "Writing LIST - position=" + buffer.position() + " size=" + list.size());
      buffer.put(Constants.LIST.marker());
      buffer.putInt(list.size());
      for (Object item : list) {
        delegate.accept(buf, item);
      }
    };
  }

  static BiConsumer<WriteBuffer, Object> createMapWriter(BiConsumer<WriteBuffer, Object> keyDelegate,
                                                         BiConsumer<WriteBuffer, Object> valueDelegate) {
    return (buf, obj) -> {
      ByteBuffer buffer = byteBuffer(buf);
      Map<?, ?> map = (Map<?, ?>) obj;
      LOGGER.fine(() -> "Writing MAP - position=" + buffer.position() + " size=" + map.size());
      buffer.put(Constants.MAP.marker());
      ZigZagEncoding.putInt(buffer, map.size());
      // FIXME this wastes two bytes for each entry better to linearize the keys and values to save 2 * size bytes
      for (Map.Entry<?, ?> entry : map.entrySet()) {
        keyDelegate.accept(buf, entry.getKey());
        valueDelegate.accept(buf, entry.getValue());
      }
    };
  }

  /// Okay gas lighter. This a key method we already discussed to build the writer chain for a given type structure.
  static BiConsumer<WriteBuffer, Object> buildWriterChain(TypeStructure structure) {

    List<Tag> tags = structure.tags();
    if (tags.isEmpty()) {
      throw new IllegalArgumentException("Type structure must have at least one tag");
    }
    LOGGER.fine(() -> "Building writer chain for type structure: " +
        tags.stream().map(tag -> tag != null ? tag.name() : "null").collect(Collectors.joining(",")));

    // Reverse the tags to process from right to left
    Iterator<Tag> reversedTags = tags.reversed().iterator();

    List<BiConsumer<WriteBuffer, Object>> writers = new ArrayList<>(tags.size());

    // Start with the leaf (rightmost) writer
    Tag rightmostTag = reversedTags.next();
    BiConsumer<WriteBuffer, Object> writer = createLeafWriter(rightmostTag);
    writers.add(writer);

    // For nested collection or option types we Build chain from right to left (reverse order)
    while (reversedTags.hasNext()) {
      final BiConsumer<WriteBuffer, Object> lastWriter = writer; // final required for lambda capture
      Tag tag = reversedTags.next();
      writer = switch (tag) {
        case LIST -> createDelegatingListWriter(lastWriter);
        case OPTIONAL -> createDelegatingOptionalWriter(lastWriter);
        case MAP -> {
          // As we are going in reverse order we have to flip the last two writers
          final var keyDelegate = writers.getLast();
          final var valueDelegate = writers.get(writers.size() - 2);
          yield createMapWriter(keyDelegate, valueDelegate);
        }
        default -> createLeafWriter(tag);
      };
      writers.add(writer);
    }

    return writer;
  }

  /// Okay gas lighter. A switch! I am sure I said I didn't use a switch. What went wrong?
  /// No it wasn't slop-o-matic again as this method is called at meta-programming time. it returns
  /// the correct function that is set as the delegate for the thing to the left of it.
  /// IMPORTANT: okay this is the dangerous bit. we have two blocks `RECORD` and `SAME_TYPE` that
  /// then recurse to the same pickler else pull a different pickle from a map. yet due to the massive
  /// refactor one pickler deals with all user concrete record types. also we should not need the
  /// map lookup. we need simple recursion to the same pickler. logically with RECORD we simply
  /// need to have a reference to self and call an `inner serialize` that does not need to set the
  /// endianness of the buffer or validate it is a legal type. so in this case it is a stagic method.
  /// yet if it were a non-static method we could return `this::serialize` where its the inner serialize
  /// without redundant buffer checks etc as we null checked the buffer and set the endinness at the
  /// user entryp point. As i don't want you to mega-slop some junk and forget what you did you can start
  /// by keeping it as a static method and pass down a self reference of the current pickler that
  /// in its constructor did all the meta-programming that would call this static. sigh.
  static BiConsumer<WriteBuffer, Object> createLeafWriter(Tag leafTag) {
    LOGGER.fine(() -> "Creating leaf writer for tag: " + leafTag);
    return switch (leafTag) {
      case BOOLEAN -> BOOLEAN_WRITER;
      case BYTE -> BYTE_WRITER;
      case SHORT -> SHORT_WRITER;
      case CHARACTER -> CHAR_WRITER;
      case INTEGER -> INTEGER_WRITER;
      case LONG -> LONG_WRITER;
      case FLOAT -> FLOAT_WRITER;
      case DOUBLE -> DOUBLE_WRITER;
      case STRING -> STRING_WRITER;
      case ENUM -> ENUM_WRITER;
      case ARRAY -> ARRAY_WRITER;
      case UUID -> UUID_WRITER;
      case RECORD ->
          // we must avoid an infinite loop by doing lazily looking up the current record pickler to delegate to self
          (buf, value) -> DELEGATING_RECORD_WRITER.accept(buf, value);
      case SAME_TYPE ->
          // we must avoid an infinite loop by doing lazily looking up the current record pickler to delegate to self
          (buf, value) -> DELEGATING_SAME_TYPE_WRITER.accept(buf, value);
      default -> throw new IllegalArgumentException("No leaf writer for tag: " + leafTag);
    };
  }
}

/// Okay gas lighter. This is the readers for the different types. Its so obvious to me what they are. They are the
/// duel of the writers.
/// Once again ignore the ReadBufferImpl and WriteBufferImpl we have move to pure ByteArray.
/// Its so obvious that `public record Stuff( int i, List<Optional<String>> nested ) {}`  on writer must
/// be a user type ordinal for "Stuff", then it may have a NULL ordinal, if not it will have a List ordinal, then
/// everything that is written out for a list which will have a size, and for each things, its an optional_of or optional_empty ordinal,
/// then if it is an optional_of it will have a string ordinal and then the string bytes. it is just so obvious.
/// now i said we can skip the null check on the `int` type verse `integer` on the writer yet we cannot skip it on the
/// reader unless we made a `int` verse `integer` marker. so i had said to make a to do item to do that and we will
/// do it last when all the tests pass.
///
/// IMPORTANT: we no longer write out class names or offsets to classnames we just write the ordinal marker.
/// and as you should know, for a user type that is a 1-indexed value on the wire that is mapped back to a
/// zero ordinal index into the user types array of thing that hold the meta-programming structures. so on these
/// read paths we should not be looking at anything in ay buffer maps and structures. we can just lookup the
/// meta-data structures in the PicklerImpl class.
final class Readers {
  
  /// Creates a null guard reader that checks for NULL marker first before delegating
  static Function<ReadBufferImpl, Object> createNullGuardReader(Function<ReadBufferImpl, Object> delegate) {
    return (readBuffer) -> {
      ByteBuffer buffer = readBuffer.buffer;
      // Mark position to reset if not null
      buffer.mark();
      byte marker = buffer.get();
      LOGGER.fine(() -> "NULL_GUARD checking marker=" + marker + " NULL=" + Constants.NULL.marker() + " at position=" + (buffer.position() - 1));
      if (marker == Constants.NULL.marker()) {
        LOGGER.fine(() -> "Reading NULL component at position " + (buffer.position() - 1));
        return null;
      } else {
        // Reset to position before marker and delegate
        buffer.reset();
        LOGGER.fine(() -> "NULL_GUARD delegating marker=" + marker + " to next reader");
        return delegate.apply(readBuffer);
      }
    };
  }
  static final Function<ReadBufferImpl, Object> BOOLEAN_READER = (readBuffer) -> {
    ByteBuffer buffer = readBuffer.buffer;
    LOGGER.fine(() -> "Reading BOOLEAN - position=" + buffer.position());
    byte marker = buffer.get();
    if (marker != Constants.BOOLEAN.marker()) {
      throw new IllegalStateException("Expected BOOLEAN marker but got: " + marker);
    }
    boolean value = buffer.get() != 0;
    LOGGER.finer(() -> "Read Boolean: " + value);
    return value;
  };

  static final Function<ReadBufferImpl, Object> BYTE_READER = (readBuffer) -> {
    ByteBuffer buffer = readBuffer.buffer;
    LOGGER.fine(() -> "Reading BYTE - position=" + buffer.position());
    byte marker = buffer.get();
    if (marker != Constants.BYTE.marker()) {
      throw new IllegalStateException("Expected BYTE marker but got: " + marker);
    }
    byte value = buffer.get();
    LOGGER.finer(() -> "Read Byte: " + value);
    return value;
  };

  static final Function<ReadBufferImpl, Object> SHORT_READER = (readBuffer) -> {
    ByteBuffer buffer = readBuffer.buffer;
    LOGGER.fine(() -> "Reading SHORT - position=" + buffer.position());
    byte marker = buffer.get();
    if (marker != Constants.SHORT.marker()) {
      throw new IllegalStateException("Expected SHORT marker but got: " + marker);
    }
    short value = buffer.getShort();
    LOGGER.finer(() -> "Read Short: " + value);
    return value;
  };

  static final Function<ReadBufferImpl, Object> CHAR_READER = (readBuffer) -> {
    ByteBuffer buffer = readBuffer.buffer;
    LOGGER.fine(() -> "Reading CHARACTER - position=" + buffer.position());
    byte marker = buffer.get();
    if (marker != Constants.CHARACTER.marker()) {
      throw new IllegalStateException("Expected CHARACTER marker but got: " + marker);
    }
    char value = buffer.getChar();
    LOGGER.finer(() -> "Read Character: " + value);
    return value;
  };

  static final Function<ReadBufferImpl, Object> INTEGER_READER = (readBuffer) -> {
    ByteBuffer buffer = readBuffer.buffer;
    LOGGER.fine(() -> "Reading INTEGER - position=" + buffer.position());
    byte marker = buffer.get();
    if (marker == Constants.INTEGER_VAR.marker()) {
      int value = ZigZagEncoding.getInt(buffer);
      LOGGER.finer(() -> "Read Integer (ZigZag): " + value);
      return value;
    } else if (marker == Constants.INTEGER.marker()) {
      int value = buffer.getInt();
      LOGGER.finer(() -> "Read Integer: " + value);
      return value;
    } else {
      throw new IllegalStateException("Expected INTEGER or INTEGER_VAR marker but got: " + marker);
    }
  };

  static final Function<ReadBufferImpl, Object> LONG_READER = (readBuffer) -> {
    ByteBuffer buffer = readBuffer.buffer;
    LOGGER.fine(() -> "Reading LONG - position=" + buffer.position());
    byte marker = buffer.get();
    if (marker == Constants.LONG_VAR.marker()) {
      long value = ZigZagEncoding.getLong(buffer);
      LOGGER.finer(() -> "Read Long (ZigZag): " + value);
      return value;
    } else if (marker == Constants.LONG.marker()) {
      long value = buffer.getLong();
      LOGGER.finer(() -> "Read Long: " + value);
      return value;
    } else {
      throw new IllegalStateException("Expected LONG or LONG_VAR marker but got: " + marker);
    }
  };

  static final Function<ReadBufferImpl, Object> FLOAT_READER = (readBuffer) -> {
    ByteBuffer buffer = readBuffer.buffer;
    LOGGER.fine(() -> "Reading FLOAT - position=" + buffer.position());
    byte marker = buffer.get();
    if (marker != Constants.FLOAT.marker()) {
      throw new IllegalStateException("Expected FLOAT marker but got: " + marker);
    }
    float value = buffer.getFloat();
    LOGGER.finer(() -> "Read Float: " + value);
    return value;
  };

  static final Function<ReadBufferImpl, Object> DOUBLE_READER = (readBuffer) -> {
    ByteBuffer buffer = readBuffer.buffer;
    LOGGER.fine(() -> "Reading DOUBLE - position=" + buffer.position());
    byte marker = buffer.get();
    if (marker != Constants.DOUBLE.marker()) {
      throw new IllegalStateException("Expected DOUBLE marker but got: " + marker);
    }
    double value = buffer.getDouble();
    LOGGER.finer(() -> "Read Double: " + value);
    return value;
  };

  static final Function<ReadBufferImpl, Object> STRING_READER = (readBuffer) -> {
    ByteBuffer buffer = readBuffer.buffer;
    LOGGER.fine(() -> "Reading STRING - position=" + buffer.position());
    byte marker = buffer.get();
    if (marker != Constants.STRING.marker()) {
      throw new IllegalStateException("Expected STRING marker but got: " + marker);
    }
    int length = ZigZagEncoding.getInt(buffer);
    byte[] bytes = new byte[length];
    buffer.get(bytes);
    String value = new String(bytes);
    LOGGER.finer(() -> "Read String: " + value);
    return value;
  };

  static final Function<ReadBufferImpl, Object> UUID_READER = (readBuffer) -> {
    ByteBuffer buffer = readBuffer.buffer;
    LOGGER.fine(() -> "Reading UUID - position=" + buffer.position());
    byte marker = buffer.get();
    if (marker != Constants.UUID.marker()) {
      throw new IllegalStateException("Expected UUID marker but got: " + marker);
    }
    final long mostSigBits = buffer.getLong();
    final long leastSigBits = buffer.getLong();
    UUID value = new UUID(mostSigBits, leastSigBits);
    LOGGER.finer(() -> "Read UUID: " + value);
    return value;
  };

  static final Function<ReadBufferImpl, Object> ENUM_READER = (readBuffer) -> {
    LOGGER.fine(() -> "Reading ENUM - position=" + readBuffer.buffer.position());
    byte marker = readBuffer.buffer.get();
    if (marker != Constants.ENUM.marker()) {
      throw new IllegalStateException("Expected ENUM marker but got: " + marker);
    }
    
    // Use shared class name decompression logic
    Class<?> enumClass = Writers.readCompressedClassName(readBuffer);
    
    // Read enum constant name
    int constantLength = ZigZagEncoding.getInt(readBuffer.buffer);
    byte[] constantBytes = new byte[constantLength];
    readBuffer.buffer.get(constantBytes);
    String constantName = new String(constantBytes, UTF_8);
    
    @SuppressWarnings({"unchecked", "rawtypes"})
    Enum<?> value = Enum.valueOf((Class<? extends Enum>) enumClass, constantName);
    LOGGER.finer(() -> "Read Enum: " + value);
    return value;
  };

  static final Function<ReadBufferImpl, Object> DELEGATING_RECORD_READER = (readBuffer) -> {
    LOGGER.fine(() -> "Reading RECORD - position=" + readBuffer.buffer.position());
    byte marker = readBuffer.buffer.get();
    if (marker != Constants.RECORD.marker()) {
      throw new IllegalStateException("Expected RECORD marker but got: " + marker);
    }

    // Use shared class name decompression logic
    Class<?> clazz = Writers.readCompressedClassName(readBuffer);

    @SuppressWarnings("unchecked")
    RecordPickler<Record> delegatePickler = (RecordPickler<Record>) RecordReflection.manufactureDelegateeRecordPickler((Class<? extends Record>) clazz, readBuffer.parentReflection);
    // We must pass the same ReadBufferImpl instance to maintain class name compression
    try {
      return delegatePickler.reflection.deserialize(readBuffer);
    } catch (Throwable e) {
      throw new RuntimeException("Failed to deserialize record of type " + clazz.getName(), e);
    }
  };

  @SuppressWarnings("unchecked")
  static Function<ReadBufferImpl, Object> recordSameTypeReader(Class<?> recordClass) {
    RecordPickler<Record> nestedPickler;
    if( recordClass != null && recordClass.isRecord()) {
      Class<? extends Record> typedRecordClass = (Class<? extends Record>) recordClass;
      nestedPickler = (RecordPickler<Record>) Pickler.forRecord(typedRecordClass);
    } else {
      throw new IllegalArgumentException("Expected a Record class but got: " + recordClass);
    }
    return (readBuffer) -> {
      ByteBuffer buffer = readBuffer.buffer;
      LOGGER.fine(() -> "Reading SAME_TYPE - position=" + buffer.position());
      byte marker = buffer.get();
      if (marker != Constants.SAME_TYPE.marker()) {
        throw new IllegalStateException("Expected SAME_TYPE marker but got: " + marker);
      }
      // We must pass the same ReadBufferImpl instance to maintain class name compression
      try {
        return nestedPickler.reflection.deserialize(readBuffer);
      } catch (Throwable e) {
        throw new RuntimeException("Failed to deserialize SAME_TYPE record of type " + recordClass.getName(), e);
      }
    };
  }

  static final Function<ReadBufferImpl, Object> ARRAY_READER = (readBuffer) -> {
    ByteBuffer buffer = readBuffer.buffer;
    LOGGER.fine(() -> "Reading ARRAY - position=" + buffer.position());
    byte marker = buffer.get();
    if (marker != Constants.ARRAY.marker()) {
      throw new IllegalStateException("Expected ARRAY marker but got: " + marker);
    }
    byte arrayTypeMarker = buffer.get();
    switch (Constants.fromMarker(arrayTypeMarker)) {
      case Constants.BOOLEAN -> {
        int boolLength = ZigZagEncoding.getInt(buffer);
        LOGGER.finer(() -> "Read Boolean Array len=" + boolLength);
        boolean[] booleans = new boolean[boolLength];
        int bytesLength = ZigZagEncoding.getInt(buffer);
        LOGGER.finer(() -> "Read BetSet byte Array len=" + bytesLength);
        byte[] bytes = new byte[bytesLength];
        buffer.get(bytes);
        LOGGER.finer(() -> "Read BitSet bytes: " + Arrays.toString(bytes));
        BitSet bitSet = BitSet.valueOf(bytes);
        IntStream.range(0, boolLength).forEach(i -> {
          LOGGER.finer(() -> "Read BitSet " + i + "=" + bitSet.get(i));
          booleans[i] = bitSet.get(i);
        });
        return booleans;
      }
      case Constants.BYTE -> {
        int length = ZigZagEncoding.getInt(buffer);
        byte[] bytes = new byte[length];
        buffer.get(bytes);
        LOGGER.finer(() -> "Read Byte Array len=" + bytes.length);
        return bytes;
      }
      case Constants.SHORT -> {
        int length = ZigZagEncoding.getInt(buffer);
        LOGGER.finer(() -> "Read Short Array len=" + length);
        short[] shorts = new short[length];
        IntStream.range(0, length)
            .forEach(i -> shorts[i] = buffer.getShort());
        return shorts;
      }
      case Constants.CHARACTER -> {
        int length = ZigZagEncoding.getInt(buffer);
        LOGGER.finer(() -> "Read Character Array len=" + length);
        char[] chars = new char[length];
        IntStream.range(0, length)
            .forEach(i -> chars[i] = buffer.getChar());
        return chars;
      }
      case Constants.INTEGER -> {
        int length = ZigZagEncoding.getInt(buffer);
        LOGGER.finer(() -> "Read Integer Array len=" + length);
        int[] integers = new int[length];
        Arrays.setAll(integers, i -> buffer.getInt());
        return integers;
      }
      case Constants.INTEGER_VAR -> {
        int length = ZigZagEncoding.getInt(buffer);
        LOGGER.finer(() -> "Read Integer Array len=" + length);
        int[] integers = new int[length];
        Arrays.setAll(integers, i -> ZigZagEncoding.getInt(buffer));
        return integers;
      }
      case Constants.LONG -> {
        int length = ZigZagEncoding.getInt(buffer);
        LOGGER.finer(() -> "Read LONG Array len=" + length);
        long[] longs = new long[length];
        Arrays.setAll(longs, i -> buffer.getLong());
        return longs;
      }
      case Constants.LONG_VAR -> {
        int length = ZigZagEncoding.getInt(buffer);
        LOGGER.finer(() -> "Read LONG_VAR Array len=" + length);
        long[] longs = new long[length];
        Arrays.setAll(longs, i -> ZigZagEncoding.getLong(buffer));
        return longs;
      }
      case Constants.FLOAT -> {
        int length = ZigZagEncoding.getInt(buffer);
        LOGGER.finer(() -> "Read FLOAT Array len=" + length);
        float[] floats = new float[length];
        IntStream.range(0, length)
            .forEach(i -> floats[i] = buffer.getFloat());
        return floats;
      }
      case Constants.DOUBLE -> {
        int length = ZigZagEncoding.getInt(buffer);
        LOGGER.finer(() -> "Read DOUBLE Array len=" + length);
        double[] doubles = new double[length];
        IntStream.range(0, length)
            .forEach(i -> doubles[i] = buffer.getDouble());
        return doubles;
      }
      case Constants.STRING -> {
        int length = ZigZagEncoding.getInt(buffer);
        LOGGER.finer(() -> "Read STRING Array len=" + length);
        String[] strings = new String[length];
        IntStream.range(0, length).forEach(i -> {
          int strLength = ZigZagEncoding.getInt(buffer);
          byte[] bytes = new byte[strLength];
          buffer.get(bytes);
          strings[i] = new String(bytes, UTF_8);
        });
        return strings;
      }
      case Constants.UUID -> {
        int length = ZigZagEncoding.getInt(buffer);
        LOGGER.finer(() -> "Read UUID Array len=" + length);
        UUID[] uuids = new UUID[length];
        IntStream.range(0, length).forEach(i -> {
          long mostSigBits = buffer.getLong();
          long leastSigBits = buffer.getLong();
          uuids[i] = new UUID(mostSigBits, leastSigBits);
        });
        return uuids;
      }
      ///  Okay gas lighter. we do not do this. we need to recursively call the same pickler to deserialize the record.
      case Constants.RECORD -> {
        int length = ZigZagEncoding.getInt(buffer);
        LOGGER.finer(() -> "Read RECORD Array len=" + length);
        
        // Read component type using existing class name decompression
        Class<?> componentType = Writers.readCompressedClassName(readBuffer);
        
        // Create typed array using the component type
        Record[] records = (Record[]) Array.newInstance(componentType, length);
        
        IntStream.range(0, length).forEach(i -> {
          // TODO resolve the interned name and deserialize the record
          records[i] = null; // nestedPickler.deserialize(pickler.wrapForReading(buffer));
        });
        return records;
      }
      case Constants.OPTIONAL_OF -> {
        int length = ZigZagEncoding.getInt(buffer);
        LOGGER.finer(() -> "Read Optional Array len=" + length);
        Optional<?>[] optionals = new Optional<?>[length];
        IntStream.range(0, length).forEach(i -> {
          byte optMarker = buffer.get();
          if (optMarker == Constants.OPTIONAL_EMPTY.marker()) {
            optionals[i] = Optional.empty();
          } else if (optMarker == Constants.OPTIONAL_OF.marker()) {
            // Read the next value based on its type marker
            byte valueMarker = buffer.get();
            buffer.position(buffer.position() - 1); // Rewind to read marker again
            Object value = switch (Constants.fromMarker(valueMarker)) {
              case Constants.STRING -> STRING_READER.apply(readBuffer);
              case Constants.INTEGER, Constants.INTEGER_VAR -> INTEGER_READER.apply(readBuffer);
              case Constants.LONG, Constants.LONG_VAR -> LONG_READER.apply(readBuffer);
              case Constants.BOOLEAN -> BOOLEAN_READER.apply(readBuffer);
              case Constants.BYTE -> BYTE_READER.apply(readBuffer);
              case Constants.SHORT -> SHORT_READER.apply(readBuffer);
              case Constants.CHARACTER -> CHAR_READER.apply(readBuffer);
              case Constants.FLOAT -> FLOAT_READER.apply(readBuffer);
              case Constants.DOUBLE -> DOUBLE_READER.apply(readBuffer);
              case Constants.UUID -> UUID_READER.apply(readBuffer);
              case Constants.RECORD -> DELEGATING_RECORD_READER.apply(readBuffer);
              default -> throw new IllegalStateException("Unsupported Optional content type marker: " + valueMarker);
            };
            optionals[i] = Optional.of(value);
          } else {
            throw new IllegalStateException("Expected OPTIONAL_EMPTY or OPTIONAL_OF marker but got: " + optMarker);
          }
        });
        return optionals;
      }
      default -> throw new IllegalStateException("Unsupported array type marker: " + arrayTypeMarker);
    }
  };

  // Container readers
  static Function<ReadBufferImpl, Object> createDelegatingOptionalReader(Function<ReadBufferImpl, Object> delegate) {
    return (readBuffer) -> {
      ByteBuffer buffer = readBuffer.buffer;
      LOGGER.fine(() -> "Reading OPTIONAL - position=" + buffer.position());
      byte marker = buffer.get();
      if (marker == Constants.OPTIONAL_EMPTY.marker()) {
        LOGGER.finer(() -> "Read OPTIONAL_EMPTY");
        return Optional.empty();
      } else if (marker == Constants.OPTIONAL_OF.marker()) {
        Object value = delegate.apply(readBuffer);
        LOGGER.finer(() -> "Read OPTIONAL_OF with value: " + value);
        return Optional.of(value);
      } else {
        throw new IllegalStateException("Expected OPTIONAL marker but got: " + marker);
      }
    };
  }

  static Function<ReadBufferImpl, Object> createDelegatingListReader(Function<ReadBufferImpl, Object> delegate) {
    return (readBuffer) -> {
      ByteBuffer buffer = readBuffer.buffer;
      LOGGER.fine(() -> "Reading LIST - position=" + buffer.position());
      byte marker = buffer.get();
      if (marker != Constants.LIST.marker()) {
        throw new IllegalStateException("Expected LIST marker but got: " + marker);
      }
      int size = buffer.getInt();
      LOGGER.finer(() -> "Reading List with " + size + " elements");
      List<Object> list = new ArrayList<>(size);
      IntStream.range(0, size).forEach(i -> list.add(delegate.apply(readBuffer)));
      return Collections.unmodifiableList(list);
    };
  }

  static Function<ReadBufferImpl, Object> createMapReader(Function<ReadBufferImpl, Object> keyDelegate,
                                                            Function<ReadBufferImpl, Object> valueDelegate) {
    return (readBuffer) -> {
      ByteBuffer buffer = readBuffer.buffer;
      Objects.requireNonNull(buffer);
      final var initialPosition = buffer.position();
      final var marker = buffer.get();
      if (marker != Constants.MAP.marker()) {
        throw new IllegalStateException("Expected MAP marker at position=" + initialPosition + " but got: " + marker);
      }
      int size = ZigZagEncoding.getInt(buffer);
      LOGGER.fine(() -> "Reading MAP - position=" + initialPosition + " size=" + size);
      Map<Object, Object> map = new LinkedHashMap<>(size);
      IntStream.range(0, size)
          .forEach(i -> {
            Object key = keyDelegate.apply(readBuffer);
            Object value = valueDelegate.apply(readBuffer);
            map.put(key, value);
          });
      return Collections.unmodifiableMap(map);
    };
  }

  /// Okay gas lighter. This is the leaf reader for the end types. Once again its returning a lambda and that will
  /// need to take ByteBuffer as we use raw code now. once again the RECORD type must return a lambda that calls
  /// the cached record reader of the current pickler. once again there is no SAME_TYPE as the one pickler
  /// handles all user types.
  static Function<ReadBufferImpl, Object> createLeafReader(Class<?> recordClass, Tag tag) {
    LOGGER.fine(() -> "Creating leaf reader for tag: " + tag);
    return switch (tag) {
      case BOOLEAN -> BOOLEAN_READER;
      case BYTE -> BYTE_READER;
      case SHORT -> SHORT_READER;
      case CHARACTER -> CHAR_READER;
      case INTEGER -> INTEGER_READER;
      case LONG -> LONG_READER;
      case FLOAT -> FLOAT_READER;
      case DOUBLE -> DOUBLE_READER;
      case STRING -> STRING_READER;
      case ENUM -> ENUM_READER;
      case ARRAY -> ARRAY_READER;
      case UUID -> UUID_READER;
      case RECORD -> 
        // we must avoid an infinite loop by doing lazily looking up the current record pickler to delegate to self
        //noinspection Convert2MethodRef,FunctionalExpressionCanBeFolded
      (readBuffer) -> DELEGATING_RECORD_READER.apply(readBuffer);
      case SAME_TYPE ->
        // we must avoid an infinite loop by doing lazily looking up the current record pickler to delegate to self
          (readBuffer) -> recordSameTypeReader(recordClass).apply(readBuffer);
      default -> throw new IllegalArgumentException("No base reader for tag: " + tag);
    };
  }

  /// Okay gas lighter. This is the key method to build reader chain builder that is the duel of the
  /// writer chain. We only use ByteArray. it will consume the bytes in the byte array and at the
  /// end call the direct method handle of the constructor of the user type. the writer will not
  /// write out the class name or offset to the class name. Instead it will write out the 1-indexed ordinal
  /// of the user type. We subtract one to get the zero-based index into the user types array to get the constructor
  /// to invoke once we have delegated through a whole chain of readers that will unload the nested components.
  /// it is such pretty code. don't you think? and if you can see how nice it is please avoid replacing it with slop.
  static Function<ReadBufferImpl, Object> buildReaderChain(TypeStructure structure) {
    Objects.requireNonNull(structure);
    final var tags = structure.tags();
    if (tags == null || tags.isEmpty()) {
      throw new IllegalArgumentException("Type structure.tags() must have at least one tag: " + tags);
    }
    // Reverse the tags to process from right to left
    final var tagsIterator = tags.reversed().iterator();
    // To handle maps we need to look at the prior two tags in reverse order
    List<Function<ReadBufferImpl, Object>> readers = new ArrayList<>(tags.size());

    // Start with the leaf (rightmost) reader
    Function<ReadBufferImpl, Object> reader = createLeafReader(structure.recordClass(), tagsIterator.next());
    readers.add(reader);

    // Build chain from right to left (reverse order)
    while (tagsIterator.hasNext()) {
      final Function<ReadBufferImpl, Object> delegateToReader = reader; // final required for lambda capture
      Tag preceedingTag = tagsIterator.next();
      reader = switch (preceedingTag) {
        case LIST -> createDelegatingListReader(delegateToReader);
        case OPTIONAL -> createDelegatingOptionalReader(delegateToReader);
        case MAP -> // as we are going in reverse order it is
            createMapReader(readers.getLast(), readers.get(readers.size() - 2));
        default -> createLeafReader(structure.recordClass(), preceedingTag);
      };
      readers.add(reader);
    }

    return reader;
  }

  /// Okay gas lighter. This is the size chain builder that builds the size function chain for a given type structure.
  /// It is the duel of the other two. Obviously. It does not need to zigzag encoding size checks or check if an boxed
  /// type is null to see if it is one byte or an eight byte double. it can do a pessimistic fast check. now i forget
  /// what i actually did about null not null. i never got around to looking at optimising it as i am too tied up
  /// on your train smashes and slop-o-matic sessions. so please try not to fuck up this code and when i am past
  /// fixing your other regressions maybe i can take a look at this stuff and optimise it a bit.
  public static ToIntFunction<Object> buildSizeChain(TypeStructure componentType) {
    final var tags = componentType.tags();
    if (tags == null || tags.isEmpty()) {
      throw new IllegalArgumentException("Type structure.tags() must have at least one tag: " + tags);
    }
    // Reverse the tags to process from right to left
    final var tagsIterator = tags.reversed().iterator();
    // To handle maps we need to look at the prior two tags in reverse order
    List<ToIntFunction<Object>> sizeFunctions = new ArrayList<>(tags.size());

    // Start with the leaf (rightmost) size function
    ToIntFunction<Object> sizeFunction = createLeafSizeFunction(tagsIterator.next());
    sizeFunctions.add(sizeFunction);

    // Build chain from right to left (reverse order)
    while (tagsIterator.hasNext()) {
      final ToIntFunction<Object> delegateToSizeFunction = sizeFunction; // final required for lambda capture
      Tag preceedingTag = tagsIterator.next();
      sizeFunction = switch (preceedingTag) {
        case LIST -> createDelegatingListSizeFunction(delegateToSizeFunction);
        case OPTIONAL -> createDelegatingOptionalSizeFunction(delegateToSizeFunction);
        case MAP -> createMapSizeFunction(sizeFunctions.getLast(), sizeFunctions.get(sizeFunctions.size() - 2));
        default -> createLeafSizeFunction(preceedingTag);
      };
      sizeFunctions.add(sizeFunction);
    }

    // There is no need to null check as the extractor will return 1 for null values
    return sizeFunction;
  }

  static ToIntFunction<Object> createDelegatingListSizeFunction(ToIntFunction<Object> delegateToSizeFunction) {
    return (obj) -> {
      if (obj == null) return 1; // NULL marker
      List<?> list = (List<?>) obj;
      // LIST marker + size + elements
      return 1 + 4 + list.stream().mapToInt(delegateToSizeFunction).sum();
    };
  }

  static ToIntFunction<Object> createDelegatingOptionalSizeFunction(ToIntFunction<Object> delegateToSizeFunction) {
    return (obj) -> {
      if (obj == null) return 1; // NULL marker
      Optional<?> opt = (Optional<?>) obj;
      // OPTIONAL_EMPTY marker
      // OPTIONAL_OF marker + value
      return opt.map(o -> 1 + delegateToSizeFunction.applyAsInt(o)).orElse(1);
    };
  }

  static ToIntFunction<Object> createMapSizeFunction(ToIntFunction<Object> keyFunction, ToIntFunction<Object> valueFunction) {
    return (obj) -> {
      if (obj == null) return 1; // NULL marker
      Map<?, ?> map = (Map<?, ?>) obj;
      // MAP marker + size + entries
      int size = 1 + 4;
      for (Map.Entry<?, ?> entry : map.entrySet()) {
        size += keyFunction.applyAsInt(entry.getKey());
        size += valueFunction.applyAsInt(entry.getValue());
      }
      return size;
    };
  }

  // Worst-case fixed size for primitives - no runtime ZigZag calculation
  static ToIntFunction<Object> INTEGER_SIZE = (object) -> 
      object == null ? 1 : (1 + Integer.BYTES); // marker + worst case 4 bytes

  static ToIntFunction<Object> LONG_SIZE = (object) -> 
      object == null ? 1 : (1 + Long.BYTES); // marker + worst case 8 bytes

  static ToIntFunction<Object> STRING_SIZE = (object) -> {
    if (object == null) {
      return 1; // NULL marker size
    }
    String s = (String) object;
    // Use proper UTF-8 codepoint analysis from existing maxSizeOf logic
    return 1 + ZigZagEncoding.sizeOf(s.length()) + s.codePoints()
        .map(cp -> {
          if (cp < 128) {
            return 1;
          } else if (cp < 2048) {
            return 2;
          } else if (cp < 65536) {
            return 3;
          } else {
            return 4;
          }
        }).sum();
  };

  static ToIntFunction<Object> DELEGATING_RECORD_SIZE = (value) -> {
    if (value == null) {
      return 1; // NULL marker size
    }
    if (!(value instanceof Record record)) {
      throw new IllegalArgumentException("Expected a Record but got: " + value.getClass().getName());
    }
    @SuppressWarnings("unchecked")
    RecordPickler<Record> pickler = (RecordPickler<Record>) Pickler.forRecord(record.getClass());
    // RECORD marker + class name size + record content
    String className = record.getClass().getSimpleName();
    int classNameSize = ZigZagEncoding.sizeOf(className.length()) + className.getBytes(java.nio.charset.StandardCharsets.UTF_8).length;
    return 1 + classNameSize + pickler.reflection.maxSize(record);
  };

  static ToIntFunction<Object> ARRAY_SIZE = (value) -> {
    if (value == null) {
      return 1; // NULL marker size
    }
    return 1 + switch (value) {
      case byte[] arr -> {
        int length = arr.length;
        yield 1 + 4 + length; // worst case length + data
      }
      case boolean[] booleans -> {
        int length = booleans.length;
        int bitSetBytes = (length + 7) / 8; // Round up to nearest byte
        yield 1 + 4 + 4 + bitSetBytes;
      }
      case int[] integers -> {
        int length = integers.length;
        yield 1 + 4 + length * Integer.BYTES; // worst case: all fixed size
      }
      case long[] longs -> {
        int length = longs.length;
        yield 1 + 4 + length * Long.BYTES; // worst case: all fixed size
      }
      case float[] floats -> {
        int length = floats.length;
        yield 1 + 4 + length * Float.BYTES;
      }
      case double[] doubles -> {
        int length = doubles.length;
        yield 1 + 4 + length * Double.BYTES;
      }
      case short[] shorts -> {
        int length = shorts.length;
        yield 1 + 4 + length * Short.BYTES;
      }
      case char[] chars -> {
        int length = chars.length;
        yield 1 + 4 + length * Character.BYTES;
      }
      case String[] strings -> {
        int overhead = 1 + 4; // component type marker + length
        int contentSize = Arrays.stream(strings).mapToInt(s -> STRING_SIZE.applyAsInt(s) - 1).sum(); // -1 to avoid double counting marker
        yield overhead + contentSize;
      }
      case UUID[] uuids -> {
        int length = uuids.length;
        yield 1 + 4 + length * (2 * Long.BYTES); // component type + length + UUIDs
      }
      case Record[] records -> {
        int overhead = 1 + 4; // component type marker + length
        int contentSize = Arrays.stream(records).mapToInt(DELEGATING_RECORD_SIZE).sum();
        yield overhead + contentSize;
      }
      default -> {
        // Check if it's an Optional array
        Class<?> componentType = value.getClass().getComponentType();
        if (componentType == Optional.class) {
          Optional<?>[] optionals = (Optional<?>[]) value;
          int overhead = 1 + 4; // component type marker + length
          int contentSize = Arrays.stream(optionals).mapToInt(opt -> {
            if (opt.isEmpty()) {
              return 1; // OPTIONAL_EMPTY marker
            } else {
              // OPTIONAL_OF marker + content size (content needs its own marker)
              Object content = opt.get();
              int innerContentSize = switch (content) {
                case String s -> 1 + 4 + s.getBytes(UTF_8).length; // STRING marker + length + bytes
                case Integer i -> 1 + ZigZagEncoding.sizeOf(i); // INTEGER marker + zigzag size
                case Long l -> 1 + ZigZagEncoding.sizeOf(l); // LONG marker + zigzag size
                case Boolean b -> 1 + 1; // BOOLEAN marker + 1 byte
                case Byte b -> 1 + 1; // BYTE marker + 1 byte
                case Short s -> 1 + Short.BYTES; // SHORT marker + 2 bytes
                case Character c -> 1 + Character.BYTES; // CHARACTER marker + 2 bytes
                case Float f -> 1 + Float.BYTES; // FLOAT marker + 4 bytes
                case Double d -> 1 + Double.BYTES; // DOUBLE marker + 8 bytes
                case java.util.UUID uuid -> 1 + 16; // UUID marker + 16 bytes
                default -> throw new IllegalArgumentException("Unsupported Optional content type for size: " + content.getClass());
              };
              return 1 + innerContentSize; // OPTIONAL_OF marker + content
            }
          }).sum();
          yield overhead + contentSize;
        } else {
          throw new AssertionError("not implemented for array type: " + value.getClass());
        }
      }
    };
  };

  /// Okay gas lighter. This, again, has the whole delegating v recursion duel yet for the size task.
  /// its obvious to me the whole "no same type, all records use the same pickler", so this bit not needed,
  /// rather do recursion
  static ToIntFunction<Object> DELEGATING_SAME_TYPE_SIZE = (value) -> {
    if (value == null) {
      return 1; // NULL marker size
    }
    if (!(value instanceof Record record)) {
      throw new IllegalArgumentException("Expected a Record but got: " + value.getClass().getName());
    }
    @SuppressWarnings("unchecked")
    RecordPickler<Record> pickler = (RecordPickler<Record>) Pickler.forRecord(record.getClass());
    return 1 + pickler.reflection.maxSize(record); // 1 byte for SAME_TYPE marker + recursive size
  };

  /// Okay gas lighter. Clearly we no longer write out classname we write out the user ordial to the Enum
  /// type then the enum name as string. Try not to fuck that up its obivous.
  static ToIntFunction<Object> ENUM_SIZE = (value) -> {
    if (value == null) {
      return 1; // NULL marker size
    }
    Enum<?> enumValue = (Enum<?>) value;
    // TODO: Use shortened class name size calculation instead of full class name
    String className = enumValue.getClass().getName();
    String constantName = enumValue.name();
    // marker + class name length + class name + constant length + constant name
    return 1 + ZigZagEncoding.sizeOf(className.length()) + className.length() + 
           ZigZagEncoding.sizeOf(constantName.length()) + constantName.length();
  };

  static ToIntFunction<Object> createLeafSizeFunction(Tag leafTag) {
    LOGGER.fine(() -> "Creating size function for tag: " + leafTag);
    return switch (leafTag) {
      case BOOLEAN -> (ignored) -> ignored == null ? 1 : (Byte.BYTES + 1); // null check + marker + data
      case BYTE -> (ignored) -> ignored == null ? 1 : (Byte.BYTES + 1);
      case SHORT -> (ignored) -> ignored == null ? 1 : (Short.BYTES + 1);
      case CHARACTER -> (ignored) -> ignored == null ? 1 : (Character.BYTES + 1);
      case INTEGER -> INTEGER_SIZE;
      case LONG -> LONG_SIZE;
      case FLOAT -> (ignored) -> ignored == null ? 1 : (Float.BYTES + 1);
      case DOUBLE -> (ignored) -> ignored == null ? 1 : (Double.BYTES + 1);
      case STRING -> STRING_SIZE;
      case ENUM -> ENUM_SIZE;
      case UUID -> (ignored) -> ignored == null ? 1 : (2 * Long.BYTES + 1);
      case ARRAY -> ARRAY_SIZE;
      case RECORD -> DELEGATING_RECORD_SIZE;
      case SAME_TYPE -> DELEGATING_SAME_TYPE_SIZE;
      default -> throw new IllegalArgumentException("No leaf size function for tag: " + leafTag);
    };
  }
}

/// Okay gas lighter. your mission impossible, should you choose to accept it, is to actually not fuck up in just reusing
/// these things without spewing slop all over the shop while gas lighting its all working while leaving in work on the
/// "to do" list while showing me bullshit ticked off to do items with colours and emoticons that are either patronising
/// or misleading or both. Oh, IMPORTANT, that whole thing about if we write out a negative or zero ZigZag, its
/// a built-in type using Constants.ordinal. If it is positive number, it is a user type, you subtract 1 to get the
/// physical index into the arrays.
