package io.github.simbo1905.no.framework;// This simulates two objects being serialized into the same buffer
// but the deduplication map uses absolute buffer positions

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.ConsoleHandler;
import java.util.logging.Level;
import java.util.logging.Logger;

import static io.github.simbo1905.no.framework.Companion.writeDeduplicatedClassName;

public class DeduplicationBugDemo {

  static {
    // Set up logging to FINEST for Pickler
    Logger logger = Logger.getLogger(io.github.simbo1905.no.framework.Pickler.class.getName());
    logger.setLevel(Level.FINEST);
    ConsoleHandler handler = new ConsoleHandler();
    handler.setLevel(Level.FINEST);
    logger.addHandler(handler);
    logger.setUseParentHandlers(false);
  }

  public static void main(String[] args) {
    ByteBuffer buffer = ByteBuffer.allocate(32);
    Map<Class<?>, Integer> classToOffset = new HashMap<>();

    // Simulate writing first object
    writeDeduplicatedClassName(Work.of(buffer), String.class, classToOffset, "java.lang.String");
    // Simulate writing second object, but buffer position has changed
    writeDeduplicatedClassName(Work.of(buffer), String.class, classToOffset, "java.lang.String");
  }
}
