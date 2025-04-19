package io.github.simbo1905.simple_pickle;

import java.lang.invoke.MethodHandle;
import java.nio.ByteBuffer;
import java.util.Arrays;

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
      default -> throw new UnsupportedOperationException("Unsupported type: " + c.getClass());
    };
  }

  public static void write(ByteBuffer buffer, Object c) {
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
      default -> throw new UnsupportedOperationException("Unsupported type: " + c.getClass());
    }
  }

  @Override
  public R deserialize(ByteBuffer buffer) {
    final var length = buffer.get();
    final var components = new Object[length];
    Arrays.setAll(components, _ -> {
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
        default -> throw new UnsupportedOperationException("Unsupported type: " + type);
      };
    });
    return this.staticCreateFromComponents(components);
  }
}
