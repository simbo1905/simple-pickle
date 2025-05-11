package io.github.simbo1905.no.framework;

import java.nio.ByteBuffer;
import java.util.Map;

public class WriteOperations {
  static void write(Map<Class<?>, Integer> classToOffset, ByteBuffer buffer, Object c) {
    throw new AssertionError("not implemented");
  }

  static Object deserializeValue(Map<Integer, Class<?>> bufferOffset2Class, ByteBuffer buffer) {
    throw new AssertionError("Not implemented");
  }
}
