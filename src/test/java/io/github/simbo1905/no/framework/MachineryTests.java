package io.github.simbo1905.no.framework;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.logging.*;
import java.util.logging.Formatter;
import java.util.stream.IntStream;

import static io.github.simbo1905.no.framework.Pickler.LOGGER;
import static org.junit.jupiter.api.Assertions.*;

public class MachineryTests {
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

  // Records for testing
  public static record DeepDouble(List<List<Optional<Double>>> deepDoubles) {}
  public static record PrimitiveRecord(boolean boolValue, byte byteValue, short shortValue,
                                       char charValue, int intValue, long longValue,
                                       float floatValue, double doubleValue) {}
  public static record WrapperRecord(Boolean boolValue, Byte byteValue, Short shortValue,
                                     Character charValue, Integer intValue, Long longValue,
                                     Float floatValue, Double doubleValue) {}
  public static record OptionalRecord(Optional<String> maybeValue) {}
  public static record ListRecord(List<String> items) {}
  public static record MapRecord(Map<String, Integer> mapping) {}
  public static record ArrayRecord(int[] numbers) {}

  @Test
  void testDeepNestedTypes() throws Throwable {
    // Analyze the record structure
    RecordReflection<DeepDouble> reflection = RecordReflection.analyze(DeepDouble.class);

    // Create test data
    DeepDouble testRecord = new DeepDouble(
        List.of(
            List.of(Optional.of(1.23), Optional.empty(), Optional.of(4.56)),
            List.of(Optional.of(7.89))
        )
    );

    // Serialize
    WriteBufferImpl writeBuffer = (WriteBufferImpl) WriteBuffer.of(1024);
    reflection.serialize(writeBuffer, testRecord);

    // Deserialize
    ByteBuffer readBuffer = writeBuffer.flip();
    DeepDouble deserializedRecord = reflection.deserialize(readBuffer);

    // Verify equality
    assertTrue(deepEquals(testRecord.deepDoubles(), deserializedRecord.deepDoubles()));
    assertEquals(testRecord, deserializedRecord);
  }

  @Test
  void testPrimitiveTypes() throws Throwable {
    RecordReflection<PrimitiveRecord> reflection = RecordReflection.analyze(PrimitiveRecord.class);

    PrimitiveRecord testRecord = new PrimitiveRecord(
        true, (byte)127, (short)32767, 'A',
        Integer.MAX_VALUE, Long.MAX_VALUE,
        3.14f, Math.PI
    );

    WriteBufferImpl writeBuffer = (WriteBufferImpl) WriteBuffer.of(1024);
    reflection.serialize(writeBuffer, testRecord);

    ByteBuffer readBuffer = writeBuffer.flip();
    PrimitiveRecord deserializedRecord = reflection.deserialize(readBuffer);

    assertEquals(testRecord, deserializedRecord);
  }

  @Test
  void testWrapperTypes() throws Throwable {
    RecordReflection<WrapperRecord> reflection = RecordReflection.analyze(WrapperRecord.class);

    WrapperRecord testRecord = new WrapperRecord(
        Boolean.TRUE, Byte.valueOf((byte)127), Short.valueOf((short)32767),
        Character.valueOf('A'), Integer.valueOf(Integer.MAX_VALUE),
        Long.valueOf(Long.MAX_VALUE), Float.valueOf(3.14f),
        Double.valueOf(Math.PI)
    );

    WriteBufferImpl writeBuffer = (WriteBufferImpl) WriteBuffer.of(1024);
    reflection.serialize(writeBuffer, testRecord);

    ByteBuffer readBuffer = writeBuffer.flip();
    WrapperRecord deserializedRecord = reflection.deserialize(readBuffer);

    assertEquals(testRecord, deserializedRecord);
  }

  @Test
  void testOptionalTypes() throws Throwable {
    RecordReflection<OptionalRecord> reflection = RecordReflection.analyze(OptionalRecord.class);

    // Test with a value
    OptionalRecord withValue = new OptionalRecord(Optional.of("Hello World"));
    WriteBufferImpl writeBuffer1 = (WriteBufferImpl) WriteBuffer.of(1024);
    reflection.serialize(writeBuffer1, withValue);
    ByteBuffer readBuffer1 = writeBuffer1.flip();
    OptionalRecord deserializedWithValue = reflection.deserialize(readBuffer1);
    assertEquals(withValue, deserializedWithValue);

    // Test empty optional
    OptionalRecord withoutValue = new OptionalRecord(Optional.empty());
    WriteBufferImpl writeBuffer2 = (WriteBufferImpl) WriteBuffer.of(1024);
    reflection.serialize(writeBuffer2, withoutValue);
    ByteBuffer readBuffer2 = writeBuffer2.flip();
    OptionalRecord deserializedWithoutValue = reflection.deserialize(readBuffer2);
    assertEquals(withoutValue, deserializedWithoutValue);
  }

  @Test
  void testListTypes() throws Throwable {
    RecordReflection<ListRecord> reflection = RecordReflection.analyze(ListRecord.class);

    ListRecord testRecord = new ListRecord(List.of("one", "two", "three"));
    WriteBufferImpl writeBuffer = (WriteBufferImpl) WriteBuffer.of(1024);
    reflection.serialize(writeBuffer, testRecord);

    ByteBuffer readBuffer = writeBuffer.flip();
    ListRecord deserializedRecord = reflection.deserialize(readBuffer);

    assertEquals(testRecord, deserializedRecord);

    // Test empty list
    ListRecord emptyList = new ListRecord(Collections.emptyList());
    WriteBufferImpl writeBuffer2 = (WriteBufferImpl) WriteBuffer.of(1024);
    reflection.serialize(writeBuffer2, emptyList);

    ByteBuffer readBuffer2 = writeBuffer2.flip();
    ListRecord deserializedEmptyList = reflection.deserialize(readBuffer2);

    assertEquals(emptyList, deserializedEmptyList);
  }

  @Test
  void testMapTypes() throws Throwable {
    RecordReflection<MapRecord> reflection = RecordReflection.analyze(MapRecord.class);

    Map<String, Integer> map = new LinkedHashMap<>();
    map.put("one", 1);
    map.put("two", 2);
    map.put("three", 3);

    MapRecord testRecord = new MapRecord(map);
    WriteBufferImpl writeBuffer = (WriteBufferImpl) WriteBuffer.of(1024);
    reflection.serialize(writeBuffer, testRecord);

    ByteBuffer readBuffer = writeBuffer.flip();
    MapRecord deserializedRecord = reflection.deserialize(readBuffer);

    assertEquals(testRecord, deserializedRecord);
  }

  @Test
  void testArrayTypes() throws Throwable {
    RecordReflection<ArrayRecord> reflection = RecordReflection.analyze(ArrayRecord.class);

    ArrayRecord testRecord = new ArrayRecord(new int[] {1, 2, 3, 4, 5});
    WriteBufferImpl writeBuffer = (WriteBufferImpl) WriteBuffer.of(1024);
    reflection.serialize(writeBuffer, testRecord);

    ByteBuffer readBuffer = writeBuffer.flip();
    ArrayRecord deserializedRecord = reflection.deserialize(readBuffer);

    assertArrayEquals(testRecord.numbers(), deserializedRecord.numbers());
  }

  static boolean deepEquals(Object a, Object b) {
    if (a == null || b == null) return a == b;

    if (a instanceof List<?> listA && b instanceof List<?> listB) {
      return listA.size() == listB.size() && IntStream.range(0, listA.size())
          .allMatch(i -> deepEquals(listA.get(i), listB.get(i)));
    }

    if (a instanceof Optional<?> optA && b instanceof Optional<?> optB) {
      return optA.equals(optB);
    }

    return a.equals(b);
  }
}
