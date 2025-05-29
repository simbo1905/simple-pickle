package io.github.simbo1905.no.framework;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
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
  void testEnum() {
    final var testEnum = new TestEnum(EnumTest.ONE);
    final var pickler = Pickler.forRecord(TestEnum.class);
    final var serializationSession = pickler.allocateForWriting(pickler.maxSizeOf(testEnum));
    pickler.serialize(serializationSession, testEnum);
    final var readBuffer = pickler.wrapForReading(serializationSession.flip());
    final var deserialized = pickler.deserialize(readBuffer);
    assertEquals(testEnum, deserialized);
  }

  @Test
  void repeatedEnumLargeOffset() {
    final var testEnumOne = new TestEnum(EnumTest.ONE);

    final var pickler = Pickler.forRecord(TestEnum.class);
    final var buf = ByteBuffer.allocate(8388608 + 128);
    final var serializationSession = pickler.wrapForWriting(buf);

    pickler.serialize(serializationSession, testEnumOne);
    buf.position(buf.position() + 8388608 + 2);

    pickler.serialize(serializationSession, testEnumOne);
    pickler.serialize(serializationSession, testEnumOne);

    final var readBuffer = pickler.wrapForReading(serializationSession.flip());
    final var deserializeOne = pickler.deserialize(readBuffer);
    assertEquals(testEnumOne, deserializeOne);

    buf.position(buf.position() + 8388608 + 2);

    final var deserializeTwo = pickler.deserialize(readBuffer);
    assertEquals(testEnumOne, deserializeTwo);

    assertEquals(testEnumOne, pickler.deserialize(readBuffer));
  }

  @Test
  void repeatedEnum() {
    final var testEnumOne = new TestEnum(EnumTest.ONE);

    final var pickler = Pickler.forRecord(TestEnum.class);
    final int size = pickler.maxSizeOf(testEnumOne) * 2;
    final var serializationSession = pickler.allocateForWriting(size);

    final var pos1 = serializationSession.position();
    pickler.serialize(serializationSession, testEnumOne);

    final var pos2 = serializationSession.position();
    pickler.serialize(serializationSession, testEnumOne);

    final var pos3 = serializationSession.position();

    final var sizeOne = pos2 - pos1;
    final var sizeTwo = pos3 - pos2;
    Assertions.assertThat(sizeTwo).isLessThan(sizeOne);

    final var readBuffer = pickler.wrapForReading(serializationSession.flip());

    final var deserializeOne = pickler.deserialize(readBuffer);
    assertEquals(testEnumOne, deserializeOne);
    final var deserializeTwo = pickler.deserialize(readBuffer);
    assertEquals(testEnumOne, deserializeTwo);
  }

  @Test
  void testEnumWithOthers() {
    final var pickler = Pickler.forRecord(TestRecord.class);
    final var serializationSession = pickler.allocateForWriting(1024);

    final var testRecord = new TestRecord("Simbo", 42, EnumTest.ONE);
    pickler.serialize(serializationSession, testRecord);
    final var testRecord2 = new TestRecord("Fido", 3, EnumTest.TWO);
    pickler.serialize(serializationSession, testRecord2);

    final var readBuffer = pickler.wrapForReading(serializationSession.flip());

    assertEquals(testRecord, pickler.deserialize(readBuffer));

    assertEquals(testRecord2, pickler.deserialize(readBuffer));
  }
}

record TestRecord(String name, int age, EnumTest testEnum) {
}

record TestEnum(EnumTest enumTest) {
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
