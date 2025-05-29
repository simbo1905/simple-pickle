package io.github.simbo1905.no.framework;

import java.lang.reflect.Type;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

class ReadBufferImpl implements ReadBuffer {
  final ByteBuffer buffer;
  boolean closed = false;

  final Function<String, Class<?>> internedNameToClass;
  final Map<Integer, Class<?>> locations = new HashMap<>(64);
  Class<?> currentRecordClass;
  
  // Legacy fields - TODO: remove when Companion.java is refactored  
  final Map<String, Enum<?>> nameToEnum = new HashMap<>(64);
  final Map<String, Class<?>> nameToClass = new HashMap<>(64);
  final List<Type[]> componentGenericTypes = new ArrayList<>();

  ReadBufferImpl(ByteBuffer buffer, Function<String, Class<?>> internedNameToClass) {
    this.internedNameToClass = internedNameToClass;
    buffer.order(ByteOrder.BIG_ENDIAN);
    this.buffer = buffer;
  }

  @Override
  public void close() throws Exception {
    this.closed = true;
  }

  @Override
  public boolean isClosed() {
    return this.closed;
  }

  @Override
  public int getVarInt() {
    if (closed) {
      throw new IllegalStateException("Buffer is closed");
    }
    return ZigZagEncoding.getInt(buffer);
  }

  @Override
  public long getVarLong() {
    if (closed) {
      throw new IllegalStateException("Buffer is closed");
    }
    return ZigZagEncoding.getLong(buffer);
  }

  @Override
  public boolean hasRemaining() {
    return buffer.hasRemaining();
  }

  @Override
  public int remaining() {
    return buffer.remaining();
  }

  String readString() {
    int length = ZigZagEncoding.getInt(buffer);
    byte[] bytes = new byte[length];
    buffer.get(bytes);
    return new String(bytes, java.nio.charset.StandardCharsets.UTF_8);
  }
}
