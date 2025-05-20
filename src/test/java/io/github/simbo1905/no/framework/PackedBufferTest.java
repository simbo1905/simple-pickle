package io.github.simbo1905.no.framework;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.logging.*;

import static io.github.simbo1905.no.framework.Pickler.LOGGER;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class PackedBufferTest {
  @BeforeAll
  static void setupLogging() {
    final var logLevel = System.getProperty("java.util.logging.ConsoleHandler.level", "FINER");
    final Level level = Level.parse(logLevel);

    // Configure the primary LOGGER instance
    LOGGER.setLevel(level);
    // Remove all existing handlers to prevent duplicates if this method is called multiple times
    // or if there are handlers configured by default.
    for (Handler handler : LOGGER.getHandlers()) {
      LOGGER.removeHandler(handler);
    }

    ConsoleHandler consoleHandler = new ConsoleHandler();
    consoleHandler.setLevel(level);

    // Create and set a custom formatter
    Formatter simpleFormatter = new Formatter() {
      @Override
      public String format(LogRecord record) {
        return record.getMessage() + "\n";
      }
    };
    consoleHandler.setFormatter(simpleFormatter);

    LOGGER.addHandler(consoleHandler);

    // Ensure parent handlers are not used to prevent duplicate logging from higher-level loggers
    LOGGER.setUseParentHandlers(false);

    LOGGER.info("Logging initialized at level: " + level);
  }

  @Test
  void testNewWorld() {
    final var pickler = Pickler.forRecord(TestRecord.class);
    final var serializationSession = Pickler.allocate(1024);

    final var testRecord = new TestRecord("Simbo", 42, EnumTest.ONE);
    pickler.serialize(serializationSession, testRecord);
    final var testRecord2 = new TestRecord("Fido", 3, EnumTest.TWO);
//    pickler.serialize(serializationSession, testRecord2);

    final var readBuffer = serializationSession.flip();

    assertEquals(testRecord, pickler.deserialize(readBuffer));

    assertEquals(testRecord2, pickler.deserialize(readBuffer));
  }
}

record TestRecord(String name, int age, EnumTest testEnum) {
}

enum EnumTest {
  ONE("Link"),
  TWO("Link2");

  private final String lower;

  EnumTest(String lower) {
    this.lower = lower;
  }

  public String lower() {
    return lower;
  }
}
