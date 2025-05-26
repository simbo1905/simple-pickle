package io.github.simbo1905.no.framework;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.lang.reflect.RecordComponent;
import java.lang.reflect.Type;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.logging.*;
import java.util.logging.Formatter;
import java.util.stream.IntStream;

import static io.github.simbo1905.no.framework.Pickler.LOGGER;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
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
  public record DeepDouble(List<List<Optional<Double>>> deepDoubles) {
  }

  public record PrimitiveRecord(boolean boolValue, byte byteValue, short shortValue,
                                char charValue, int intValue, long longValue,
                                float floatValue, double doubleValue) {
  }

  public record WrapperRecord(Boolean boolValue, Byte byteValue, Short shortValue,
                              Character charValue, Integer intValue, Long longValue,
                              Float floatValue, Double doubleValue) {
  }

  public record OptionalRecord(Optional<String> maybeValue) {
  }

  public record ListRecord(List<String> items) {
  }

  public record MapRecord(Map<Integer, String> mapping) {
  }

  public record ArrayBooleanRecord(boolean[] booleans) {
  }

  public record ArrayIntRecord(int[] integers) {
  }

  public record ArrayBytes(byte[] bytes) {
  }

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
        true, (byte) 127, (short) 32767, 'A',
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
        Boolean.TRUE, (byte) 127, (short) 32767,
        'A', Integer.MAX_VALUE,
        Long.MAX_VALUE, 3.14f,
        Math.PI
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
  void testListAnalysis() {
    // public record ListRecord(List<String> items) {}
    RecordComponent[] components = ListRecord.class.getRecordComponents();
    // Analyze type structure
    Type genericType = components[0].getGenericType();
    TypeStructure typeStructure = TypeStructure.analyze(genericType);
    assertThat(typeStructure).isNotNull();
    assertThat(typeStructure.tags().size()).isEqualTo(2);
    assertThat(typeStructure.tags().get(0)).isEqualTo(Tag.LIST);
    assertThat(typeStructure.tags().get(1)).isEqualTo(Tag.STRING);
  }

  @Test
  void testMapAnalysis() {
    // public record MapRecord(Map<Integer, String> mapping) {}
    RecordComponent[] components = MapRecord.class.getRecordComponents();
    // Analyze type structure
    Type genericType = components[0].getGenericType();
    TypeStructure typeStructure = TypeStructure.analyze(genericType);
    assertThat(typeStructure).isNotNull();
    assertThat(typeStructure.tags().size()).isEqualTo(3);
    assertThat(typeStructure.tags().get(0)).isEqualTo(Tag.MAP);
    assertThat(typeStructure.tags().get(1)).isEqualTo(Tag.INTEGER);
    assertThat(typeStructure.tags().get(2)).isEqualTo(Tag.STRING);
  }

  @Test
  void testMapTypes() throws Throwable {

    Map<Integer, String> map = new LinkedHashMap<>();
    map.put(1, "one");
    map.put(2, "two");
    map.put(3, "three");

    MapRecord testRecord = new MapRecord(map);
    WriteBufferImpl writeBuffer = (WriteBufferImpl) WriteBuffer.of(1024);

    RecordReflection<MapRecord> reflection = RecordReflection.analyze(MapRecord.class);
    reflection.serialize(writeBuffer, testRecord);

    ByteBuffer readBuffer = writeBuffer.flip();
    MapRecord deserializedRecord = reflection.deserialize(readBuffer);

    assertEquals(testRecord, deserializedRecord);
  }

  @Test
  void testArrayBytes() throws Throwable {
    RecordReflection<ArrayBytes> reflection = RecordReflection.analyze(ArrayBytes.class);

    ArrayBytes testRecord = new ArrayBytes("hello".getBytes(StandardCharsets.UTF_8));
    WriteBufferImpl writeBuffer = (WriteBufferImpl) WriteBuffer.of(1024);
    reflection.serialize(writeBuffer, testRecord);

    ByteBuffer readBuffer = writeBuffer.flip();
    ArrayBytes deserializedRecord = reflection.deserialize(readBuffer);

    assertArrayEquals(testRecord.bytes(), deserializedRecord.bytes());
  }

  @Test
  void testArrayBoolean() throws Throwable {
    RecordReflection<ArrayBooleanRecord> reflection = RecordReflection.analyze(ArrayBooleanRecord.class);

    ArrayBooleanRecord testRecord = new ArrayBooleanRecord(new boolean[]{true, false, true});
    WriteBufferImpl writeBuffer = (WriteBufferImpl) WriteBuffer.of(1024);
    reflection.serialize(writeBuffer, testRecord);

    ByteBuffer readBuffer = writeBuffer.flip();
    ArrayBooleanRecord deserializedRecord = reflection.deserialize(readBuffer);

    assertArrayEquals(testRecord.booleans(), deserializedRecord.booleans());
  }

  @Test
  void testArrayVarInt() throws Throwable {
    RecordReflection<ArrayIntRecord> reflection = RecordReflection.analyze(ArrayIntRecord.class);

    ArrayIntRecord testRecord = new ArrayIntRecord(new int[]{1, 2});
    WriteBufferImpl writeBuffer = (WriteBufferImpl) WriteBuffer.of(1024);
    reflection.serialize(writeBuffer, testRecord);

    ByteBuffer readBuffer = writeBuffer.flip();
    ArrayIntRecord deserializedRecord = reflection.deserialize(readBuffer);

    assertArrayEquals(testRecord.integers(), deserializedRecord.integers());
  }

  @Test
  void testArrayInt() throws Throwable {
    RecordReflection<ArrayIntRecord> reflection = RecordReflection.analyze(ArrayIntRecord.class);

    ArrayIntRecord testRecord = new ArrayIntRecord(new int[]{Integer.MAX_VALUE, Integer.MIN_VALUE});

    int size = reflection.maxSize(testRecord);

    WriteBufferImpl writeBuffer = (WriteBufferImpl) WriteBuffer.of(size);
    reflection.serialize(writeBuffer, testRecord);

    ByteBuffer readBuffer = writeBuffer.flip();
    ArrayIntRecord deserializedRecord = reflection.deserialize(readBuffer);

    assertArrayEquals(testRecord.integers(), deserializedRecord.integers());
  }

  @Test
  void testSmallVarIntArray() throws Throwable {
    RecordReflection<ArrayIntRecord> reflection = RecordReflection.analyze(ArrayIntRecord.class);

    ArrayIntRecord testRecord = new ArrayIntRecord(new int[]{1, 2});

    int size = reflection.maxSize(testRecord);

    WriteBufferImpl writeBuffer = (WriteBufferImpl) WriteBuffer.of(size);
    reflection.serialize(writeBuffer, testRecord);

    ByteBuffer readBuffer = writeBuffer.flip();
    ArrayIntRecord deserializedRecord = reflection.deserialize(readBuffer);

    assertArrayEquals(testRecord.integers(), deserializedRecord.integers());
  }


  @Test
  void testLargeVarIntArray() throws Throwable {
    RecordReflection<ArrayIntRecord> reflection = RecordReflection.analyze(ArrayIntRecord.class);

    int[] integers = new int[1000];
    for (int i = 0; i < integers.length; i++) {
      integers[i] = i * 1000; // Write in a way that is better suited for varint encoding
    }

    ArrayIntRecord testRecord = new ArrayIntRecord( integers );

    int size = reflection.maxSize(testRecord);

    WriteBufferImpl writeBuffer = (WriteBufferImpl) WriteBuffer.of(size);
    reflection.serialize(writeBuffer, testRecord);

    ByteBuffer readBuffer = writeBuffer.flip();
    ArrayIntRecord deserializedRecord = reflection.deserialize(readBuffer);

    assertArrayEquals(testRecord.integers(), deserializedRecord.integers());
  }

  @Test
  void testLargeIntArray() throws Throwable {
    RecordReflection<ArrayIntRecord> reflection = RecordReflection.analyze(ArrayIntRecord.class);

    Random random = new Random(1234315135L);

    int[] integers = new int[1000];
    for (int i = 0; i < integers.length; i++) {
      integers[i] = random.nextInt();
    }

    ArrayIntRecord testRecord = new ArrayIntRecord( integers );

    int size = reflection.maxSize(testRecord);

    WriteBufferImpl writeBuffer = (WriteBufferImpl) WriteBuffer.of(size);

    reflection.serialize(writeBuffer, testRecord);

    ByteBuffer readBuffer = writeBuffer.flip();
    ArrayIntRecord deserializedRecord = reflection.deserialize(readBuffer);

    assertArrayEquals(testRecord.integers(), deserializedRecord.integers());
  }

  public record UUIDRecord(UUID uuid) {
  }

  @Test
  void testUUID() throws Throwable {
    RecordReflection<UUIDRecord> reflection = RecordReflection.analyze(UUIDRecord.class);

    UUID testUUID = new UUID(12343535L, 9876543210L);
    UUIDRecord testRecord = new UUIDRecord(testUUID);

    int size = reflection.maxSize(testRecord);

    WriteBufferImpl writeBuffer = (WriteBufferImpl) WriteBuffer.of(size);
    reflection.serialize(writeBuffer, testRecord);

    ByteBuffer readBuffer = writeBuffer.flip();
    UUIDRecord deserialized = reflection.deserialize(readBuffer);

    assertEquals(testUUID, deserialized.uuid());
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
