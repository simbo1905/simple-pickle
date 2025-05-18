package io.github.simbo1905;

import io.github.simbo1905.no.framework.PackedBuffer;
import io.github.simbo1905.no.framework.Pickler;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.logging.*;

import static io.github.simbo1905.no.framework.Pickler.LOGGER;
import static io.github.simbo1905.no.framework.Pickler.forRecord;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class RefactorTests2 {

  @BeforeAll
  static void setupLogging() {
    final var logLevel = System.getProperty("java.util.logging.ConsoleHandler.level", "FINER");
    final Level level = Level.parse(logLevel);

    // Configure the primary LOGGER instance
    LOGGER.setLevel(level);
    // Remove all existing handlers to prevent duplicates
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
    LOGGER.setUseParentHandlers(false);
    LOGGER.info("Logging initialized at level: " + level);
  }

  // Test record for byte
  public record ByteRecord(byte value) {}

  @Test
  void testByteRecordSerialization() {
    // Arrange
    ByteRecord record = new ByteRecord((byte)127);
    Pickler<ByteRecord> pickler = forRecord(ByteRecord.class);
    PackedBuffer buffer = Pickler.allocate(16);

    // Act
    pickler.serialize(buffer, record);
    final var buf = buffer.flip();
    ByteRecord deserialized = pickler.deserialize(buf);

    // Assert
    assertEquals(record, deserialized, "Byte record should be preserved");
  }

  // Test record for short
  public record ShortRecord(short value) {}

  @Test
  void testShortRecordSerialization() {
    // Arrange
    ShortRecord record = new ShortRecord((short)32767);
    Pickler<ShortRecord> pickler = forRecord(ShortRecord.class);
    PackedBuffer buffer = Pickler.allocate(16);

    // Act
    pickler.serialize(buffer, record);
    final var buf = buffer.flip();
    ShortRecord deserialized = pickler.deserialize(buf);

    // Assert
    assertEquals(record, deserialized, "Short record should be preserved");
  }

  // Test record for float
  public record FloatRecord(float value) {}

  @Test
  void testFloatRecordSerialization() {
    // Arrange
    FloatRecord record = new FloatRecord(3.14159f);
    Pickler<FloatRecord> pickler = forRecord(FloatRecord.class);
    PackedBuffer buffer = Pickler.allocate(16);

    // Act
    pickler.serialize(buffer, record);
    final var buf = buffer.flip();
    FloatRecord deserialized = pickler.deserialize(buf);

    // Assert
    assertEquals(record, deserialized, "Float record should be preserved");
  }

  // Test record for double
  public record DoubleRecord(double value) {}

  @Test
  void testDoubleRecordSerialization() {
    // Arrange
    DoubleRecord record = new DoubleRecord(Math.PI);
    Pickler<DoubleRecord> pickler = forRecord(DoubleRecord.class);
    PackedBuffer buffer = Pickler.allocate(16);

    // Act
    pickler.serialize(buffer, record);
    final var buf = buffer.flip();
    DoubleRecord deserialized = pickler.deserialize(buf);

    // Assert
    assertEquals(record, deserialized, "Double record should be preserved");
  }

  // Test record for Optional<Byte>
  public record OptionalByteRecord(Optional<Byte> value) {}

  @Test
  void testOptionalByteRecordSerialization() {
    // Arrange
    OptionalByteRecord record = new OptionalByteRecord(Optional.of((byte)42));
    Pickler<OptionalByteRecord> pickler = forRecord(OptionalByteRecord.class);
    PackedBuffer buffer = Pickler.allocate(32);

    // Act - Present value
    pickler.serialize(buffer, record);
    final var buf = buffer.flip();
    OptionalByteRecord deserialized = pickler.deserialize(buf);

    // Assert
    assertEquals(record, deserialized, "Optional<Byte> record with value should be preserved");

    // Test empty optional
    buffer = Pickler.allocate(32);
    OptionalByteRecord emptyRecord = new OptionalByteRecord(Optional.empty());
    pickler.serialize(buffer, emptyRecord);
    final var buf2 = buffer.flip();
    OptionalByteRecord deserializedEmpty = pickler.deserialize(buf2);

    assertEquals(emptyRecord, deserializedEmpty, "Empty Optional<Byte> should be preserved");
  }

  // Test record for byte array
  public record ByteArrayRecord(byte[] values) {
    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      ByteArrayRecord that = (ByteArrayRecord) o;
      return java.util.Arrays.equals(values, that.values);
    }

    @Override
    public int hashCode() {
      return java.util.Arrays.hashCode(values);
    }
  }

  @Test
  void testByteArrayRecordSerialization() {
    // Arrange
    ByteArrayRecord record = new ByteArrayRecord(new byte[]{1, 2, 3, 127, -128});
    Pickler<ByteArrayRecord> pickler = forRecord(ByteArrayRecord.class);
    PackedBuffer buffer = Pickler.allocate(64);

    // Act
    pickler.serialize(buffer, record);
    final var buf = buffer.flip();
    ByteArrayRecord deserialized = pickler.deserialize(buf);

    // Assert
    assertEquals(record, deserialized, "Byte array record should be preserved");
    assertArrayEquals(record.values(), deserialized.values(), "Byte array contents should match");
  }

  // Test record for short array
  public record ShortArrayRecord(short[] values) {
    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      ShortArrayRecord that = (ShortArrayRecord) o;
      return java.util.Arrays.equals(values, that.values);
    }

    @Override
    public int hashCode() {
      return java.util.Arrays.hashCode(values);
    }
  }

  @Test
  void testShortArrayRecordSerialization() {
    // Arrange
    ShortArrayRecord record = new ShortArrayRecord(new short[]{100, 200, 300, 32767, -32768});
    Pickler<ShortArrayRecord> pickler = forRecord(ShortArrayRecord.class);
    PackedBuffer buffer = Pickler.allocate(64);

    // Act
    pickler.serialize(buffer, record);
    final var buf = buffer.flip();
    ShortArrayRecord deserialized = pickler.deserialize(buf);

    // Assert
    assertEquals(record, deserialized, "Short array record should be preserved");
    assertArrayEquals(record.values(), deserialized.values(), "Short array contents should match");
  }

  // Test record for float array
  public record FloatArrayRecord(float[] values) {
    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      FloatArrayRecord that = (FloatArrayRecord) o;
      return java.util.Arrays.equals(values, that.values);
    }

    @Override
    public int hashCode() {
      return java.util.Arrays.hashCode(values);
    }
  }

  @Test
  void testFloatArrayRecordSerialization() {
    // Arrange
    FloatArrayRecord record = new FloatArrayRecord(new float[]{1.1f, 2.2f, 3.3f, -0.1f, Float.MAX_VALUE});
    Pickler<FloatArrayRecord> pickler = forRecord(FloatArrayRecord.class);
    PackedBuffer buffer = Pickler.allocate(64);

    // Act
    pickler.serialize(buffer, record);
    final var buf = buffer.flip();
    FloatArrayRecord deserialized = pickler.deserialize(buf);

    // Assert
    assertEquals(record, deserialized, "Float array record should be preserved");
    assertArrayEquals(record.values(), deserialized.values(), "Float array contents should match");
  }

  // Test record for double array
  public record DoubleArrayRecord(double[] values) {
    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      DoubleArrayRecord that = (DoubleArrayRecord) o;
      return java.util.Arrays.equals(values, that.values);
    }

    @Override
    public int hashCode() {
      return java.util.Arrays.hashCode(values);
    }
  }

  @Test
  void testDoubleArrayRecordSerialization() {
    // Arrange
    DoubleArrayRecord record = new DoubleArrayRecord(new double[]{Math.PI, Math.E, 1.0/3.0, Double.MIN_VALUE, Double.MAX_VALUE});
    Pickler<DoubleArrayRecord> pickler = forRecord(DoubleArrayRecord.class);
    PackedBuffer buffer = Pickler.allocate(128);

    // Act
    pickler.serialize(buffer, record);
    final var buf = buffer.flip();
    DoubleArrayRecord deserialized = pickler.deserialize(buf);

    // Assert
    assertEquals(record, deserialized, "Double array record should be preserved");
    assertArrayEquals(record.values(), deserialized.values(), "Double array contents should match");
  }
}
