package io.github.simbo1905.simple_pickle;

import java.lang.invoke.MethodHandle;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Optional;

import static java.nio.charset.StandardCharsets.UTF_8;

abstract public class PicklerBase<R extends Record> implements Pickler<R> {
  protected MethodHandle[] componentAccessors;

  protected MethodHandle constructorHandle;

  abstract Object[] staticGetComponents(R record);

  abstract R staticCreateFromComponents(Object[] record);

  @Override
  public void serialize(R object, ByteBuffer buffer) {
    final var components = staticGetComponents(object);
    buffer.put((byte) components.length);
    Arrays.stream(components).forEach(c -> write(buffer, c));
  }

  public static byte typeMarker(Object c) {
    if (c == null) {
      return 11; // 11 is for null values
    }
    return switch (c) {
      case Integer _ -> 0;
      case Long _ -> 1;
      case Short _ -> 2;
      case Byte _ -> 3;
      case Double _ -> 4;
      case Float _ -> 5;
      case Character _ -> 6;
      case Boolean _ -> 7;
      case String _ -> 8;
      case Optional<?> _ -> 9;
      case Record _ -> 10; // 10 is for nested records
      default -> throw new UnsupportedOperationException("Unsupported type: " + c.getClass());
    };
  }

  public static void write(ByteBuffer buffer, Object c) {
    if (c == null) {
      buffer.put((byte) 11); // 11 is for null values
      return;
    }

    switch (c) {
      case Integer i -> buffer.put(typeMarker(c)).putInt(i);
      case Long l -> buffer.put(typeMarker(c)).putLong(l);
      case Short s -> buffer.put(typeMarker(c)).putShort(s);
      case Byte b -> buffer.put(typeMarker(c)).put(b);
      case Double d -> buffer.put(typeMarker(c)).putDouble(d);
      case Float f -> buffer.put(typeMarker(c)).putFloat(f);
      case Character ch -> buffer.put(typeMarker(c)).putChar(ch);
      case Boolean bool -> buffer.put(typeMarker(c)).put((byte) (bool ? 1 : 0));
      case String str -> {
        buffer.put(typeMarker(c));
        final var bytes = str.getBytes(UTF_8);
        buffer.put((byte) bytes.length);
        buffer.put(bytes);
      }
      case Optional<?> opt -> {
        buffer.put(typeMarker(c));
        if (opt.isEmpty()) {
          buffer.put((byte) 0); // 0 means empty
        } else {
          buffer.put((byte) 1); // 1 means present
          Object value = opt.get();
          write(buffer, value);
        }
      }
      case Record record -> {
        buffer.put(typeMarker(c));
        // Write the class name for deserialization
        String className = record.getClass().getName();
        byte[] classNameBytes = className.getBytes(UTF_8);
        buffer.put((byte) classNameBytes.length);
        buffer.put(classNameBytes);

        // Get the appropriate pickler for this record type
        @SuppressWarnings("unchecked")
        Pickler<Record> nestedPickler = (Pickler<Record>) Pickler.picklerForRecord(record.getClass());

        // Use that pickler to serialize the nested record
        nestedPickler.serialize(record, buffer);
      }
      default -> throw new UnsupportedOperationException("Unsupported type: " + c.getClass());
    }
  }

  @Override
  public R deserialize(ByteBuffer buffer) {
    final var length = buffer.get();
    final var components = new Object[length];
    Arrays.setAll(components, _ -> deserializeValue(buffer));
    return this.staticCreateFromComponents(components);
  }

  private Object deserializeValue(ByteBuffer buffer) {
    final byte type = buffer.get();
    return switch (type) {
      case 0 -> buffer.getInt();
      case 1 -> buffer.getLong();
      case 2 -> buffer.getShort();
      case 3 -> buffer.get();
      case 4 -> buffer.getDouble();
      case 5 -> buffer.getFloat();
      case 6 -> buffer.getChar();
      case 7 -> buffer.get() == 1;
      case 8 -> {
        final var strLength = buffer.get();
        final byte[] bytes = new byte[strLength];
        buffer.get(bytes);
        yield new String(bytes, UTF_8);
      }
      case 9 -> {
        byte isPresent = buffer.get();
        if (isPresent == 0) {
          yield Optional.empty();
        } else {
          Object value = deserializeValue(buffer);
          yield Optional.of(value);
        }
      }
      case 10 -> { // Handle nested record
        // Read the class name
        byte classNameLength = buffer.get();
        byte[] classNameBytes = new byte[classNameLength];
        buffer.get(classNameBytes);
        String className = new String(classNameBytes, UTF_8);

        try {
          // Load the class
          @SuppressWarnings("unchecked")
          Class<? extends Record> recordClass = (Class<? extends Record>) Class.forName(className);

          // Get or create the pickler for this class
          @SuppressWarnings("unchecked")
          Pickler<Record> nestedPickler = (Pickler<Record>) Pickler.picklerForRecord(recordClass);

          // Deserialize the nested record
          yield nestedPickler.deserialize(buffer);
        } catch (ClassNotFoundException e) {
          throw new RuntimeException("Failed to load class: " + className, e);
        }
      }
      case 11 -> null; // Handle null values
      default -> throw new UnsupportedOperationException("Unsupported type: " + type);
    };
  }
}
