package io.github.simbo1905.no.framework;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.lang.reflect.RecordComponent;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.logging.*;
import java.util.logging.Formatter;
import java.util.stream.IntStream;

import static io.github.simbo1905.no.framework.Pickler.LOGGER;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.jupiter.api.Assertions.*;

public class MachineryTests {
  
  // Helper method to create RecordPickler for tests
  private static <R extends Record> RecordPickler<R> createPickler(Class<R> recordClass) {
    return new RecordPickler<>(recordClass);
  }
  
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

  public record ArrayLongRecord(long[] longs) {
  }

  public record ArrayBytes(byte[] bytes) {
  }

  @Test
  void testDeepNestedTypes() {
    // Create pickler
    RecordPickler<DeepDouble> pickler = createPickler(DeepDouble.class);

    // Create test data
    DeepDouble testRecord = new DeepDouble(
        List.of(
            List.of(Optional.of(1.23), Optional.empty(), Optional.of(4.56)),
            List.of(Optional.of(7.89))
        )
    );

    // Serialize
    WriteBuffer writeBuffer = pickler.allocateForWriting(pickler.maxSizeOf (testRecord));
    pickler.serialize(writeBuffer, testRecord);

    // Deserialize
    ReadBuffer readBuffer = pickler.wrapForReading(writeBuffer.flip());
    DeepDouble deserializedRecord = pickler.deserialize(readBuffer);

    // Verify equality
    assertTrue(deepEquals(testRecord.deepDoubles(), deserializedRecord.deepDoubles()));
    assertEquals(testRecord, deserializedRecord);
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

  @Test
  void testPrimitiveTypes() {
    RecordPickler<PrimitiveRecord> pickler = createPickler(PrimitiveRecord.class);

    PrimitiveRecord testRecord = new PrimitiveRecord(
        true, (byte) 127, (short) 32767, 'A',
        Integer.MAX_VALUE, Long.MAX_VALUE,
        3.14f, Math.PI
    );

    WriteBuffer writeBuffer = pickler.allocateForWriting(pickler.maxSizeOf(testRecord));
    pickler.serialize(writeBuffer, testRecord);

    ReadBuffer readBuffer = pickler.wrapForReading(writeBuffer.flip());
    PrimitiveRecord deserializedRecord = pickler.deserialize(readBuffer);

    assertEquals(testRecord, deserializedRecord);
  }

  @Test
  void testWrapperTypes() {
    RecordPickler<WrapperRecord> pickler = createPickler(WrapperRecord.class);

    WrapperRecord testRecord = new WrapperRecord(
        Boolean.TRUE, (byte) 127, (short) 32767,
        'A', Integer.MAX_VALUE,
        Long.MAX_VALUE, 3.14f,
        Math.PI
    );

    WriteBuffer writeBuffer = pickler.allocateForWriting(pickler.maxSizeOf (testRecord));
    pickler.serialize(writeBuffer, testRecord);

    ReadBuffer readBuffer = pickler.wrapForReading(writeBuffer.flip());
    WrapperRecord deserializedRecord = pickler.deserialize(readBuffer);

    assertEquals(testRecord, deserializedRecord);
  }

  @Test
  void testOptionalTypes() {
    RecordPickler<OptionalRecord> pickler = createPickler(OptionalRecord.class);

    // Test with a value
    OptionalRecord withValue = new OptionalRecord(Optional.of("Hello World"));
    WriteBuffer writeBuffer1 = pickler.allocateForWriting(pickler.maxSizeOf (withValue));
    pickler.serialize(writeBuffer1, withValue);
    ReadBuffer readBuffer1 = pickler.wrapForReading(writeBuffer1.flip());
    OptionalRecord deserializedWithValue = pickler.deserialize(readBuffer1);
    assertEquals(withValue, deserializedWithValue);

    // Test empty optional
    OptionalRecord withoutValue = new OptionalRecord(Optional.empty());
    WriteBuffer writeBuffer2 = pickler.allocateForWriting(pickler.maxSizeOf (withoutValue));
    pickler.serialize(writeBuffer2, withoutValue);
    ReadBuffer readBuffer2 = pickler.wrapForReading(writeBuffer2.flip());
    OptionalRecord deserializedWithoutValue = pickler.deserialize(readBuffer2);
    assertEquals(withoutValue, deserializedWithoutValue);
  }

  @Test
  void testListTypes() throws Throwable {
    RecordPickler<ListRecord> pickler = createPickler(ListRecord.class);

    ListRecord testRecord = new ListRecord(List.of("one", "two", "three"));
    WriteBuffer writeBuffer = pickler.allocateForWriting(pickler.maxSizeOf (testRecord));
    pickler.serialize(writeBuffer, testRecord);

    ReadBuffer readBuffer = pickler.wrapForReading(writeBuffer.flip());
    ListRecord deserializedRecord = pickler.deserialize(readBuffer);

    assertEquals(testRecord, deserializedRecord);

    // Test empty list
    ListRecord emptyList = new ListRecord(Collections.emptyList());
    WriteBuffer writeBuffer2 = pickler.allocateForWriting(pickler.maxSizeOf (emptyList));
    pickler.serialize(writeBuffer2, emptyList);

    ReadBuffer readBuffer2 = pickler.wrapForReading(writeBuffer2.flip());
    ListRecord deserializedEmptyList = pickler.deserialize(readBuffer2);

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
    RecordPickler<MapRecord> pickler = createPickler(MapRecord.class);
    WriteBuffer writeBuffer = pickler.allocateForWriting(pickler.maxSizeOf (testRecord));
    pickler.serialize(writeBuffer, testRecord);

    ReadBuffer readBuffer = pickler.wrapForReading(writeBuffer.flip());
    MapRecord deserializedRecord = pickler.deserialize(readBuffer);

    assertEquals(testRecord, deserializedRecord);
  }

  @Test
  void testArrayBytes() throws Throwable {
    RecordPickler<ArrayBytes> pickler = createPickler(ArrayBytes.class);

    ArrayBytes testRecord = new ArrayBytes("hello".getBytes(StandardCharsets.UTF_8));
    WriteBuffer writeBuffer = pickler.allocateForWriting(pickler.maxSizeOf (testRecord));
    pickler.serialize(writeBuffer, testRecord);

    ReadBuffer readBuffer = pickler.wrapForReading(writeBuffer.flip());
    ArrayBytes deserializedRecord = pickler.deserialize(readBuffer);

    assertArrayEquals(testRecord.bytes(), deserializedRecord.bytes());
  }

  @Test
  void testArrayBoolean() throws Throwable {
    RecordPickler<ArrayBooleanRecord> pickler = createPickler(ArrayBooleanRecord.class);

    ArrayBooleanRecord testRecord = new ArrayBooleanRecord(new boolean[]{true, false, true});
    WriteBuffer writeBuffer = pickler.allocateForWriting(pickler.maxSizeOf (testRecord));
    pickler.serialize(writeBuffer, testRecord);

    ReadBuffer readBuffer = pickler.wrapForReading(writeBuffer.flip());
    ArrayBooleanRecord deserializedRecord = pickler.deserialize(readBuffer);

    assertArrayEquals(testRecord.booleans(), deserializedRecord.booleans());
  }

  @Test
  void testArrayVarInt() throws Throwable {
    RecordPickler<ArrayIntRecord> pickler = createPickler(ArrayIntRecord.class);

    ArrayIntRecord testRecord = new ArrayIntRecord(new int[]{1, 2});
    WriteBuffer writeBuffer = pickler.allocateForWriting(pickler.maxSizeOf (testRecord));
    pickler.serialize(writeBuffer, testRecord);

    ReadBuffer readBuffer = pickler.wrapForReading(writeBuffer.flip());
    ArrayIntRecord deserializedRecord = pickler.deserialize(readBuffer);

    assertArrayEquals(testRecord.integers(), deserializedRecord.integers());
  }

  @Test
  void testArrayInt() throws Throwable {
    RecordPickler<ArrayIntRecord> pickler = createPickler(ArrayIntRecord.class);

    ArrayIntRecord testRecord = new ArrayIntRecord(new int[]{Integer.MAX_VALUE, Integer.MIN_VALUE});

    WriteBuffer writeBuffer = pickler.allocateForWriting(pickler.maxSizeOf (testRecord));
    pickler.serialize(writeBuffer, testRecord);

    ReadBuffer readBuffer = pickler.wrapForReading(writeBuffer.flip());
    ArrayIntRecord deserializedRecord = pickler.deserialize(readBuffer);

    assertArrayEquals(testRecord.integers(), deserializedRecord.integers());
  }

  @Test
  void testSmallVarIntArray() throws Throwable {
    RecordPickler<ArrayIntRecord> pickler = createPickler(ArrayIntRecord.class);

    ArrayIntRecord testRecord = new ArrayIntRecord(new int[]{1, 2});

    WriteBuffer writeBuffer = pickler.allocateForWriting(pickler.maxSizeOf (testRecord));
    pickler.serialize(writeBuffer, testRecord);

    ReadBuffer readBuffer = pickler.wrapForReading(writeBuffer.flip());
    ArrayIntRecord deserializedRecord = pickler.deserialize(readBuffer);

    assertArrayEquals(testRecord.integers(), deserializedRecord.integers());
  }


  @Test
  void testLargeVarIntArray() throws Throwable {
    RecordPickler<ArrayIntRecord> pickler = createPickler(ArrayIntRecord.class);

    int[] integers = new int[1000];
    for (int i = 0; i < integers.length; i++) {
      integers[i] = i * 1000; // Write in a way that is better suited for varint encoding
    }

    ArrayIntRecord testRecord = new ArrayIntRecord( integers );

    WriteBuffer writeBuffer = pickler.allocateForWriting(pickler.maxSizeOf (testRecord));
    pickler.serialize(writeBuffer, testRecord);

    ReadBuffer readBuffer = pickler.wrapForReading(writeBuffer.flip());
    ArrayIntRecord deserializedRecord = pickler.deserialize(readBuffer);

    assertArrayEquals(testRecord.integers(), deserializedRecord.integers());
  }

  @Test
  void testLargeIntArray() throws Throwable {
    RecordPickler<ArrayIntRecord> pickler = createPickler(ArrayIntRecord.class);

    Random random = new Random(1234315135L);

    int[] integers = new int[1000];
    for (int i = 0; i < integers.length; i++) {
      // random longs will flip a load of bits so be too large for varint encoding
      integers[i] = random.nextInt();
    }

    ArrayIntRecord testRecord = new ArrayIntRecord( integers );

    WriteBuffer writeBuffer = pickler.allocateForWriting(pickler.maxSizeOf (testRecord));
    pickler.serialize(writeBuffer, testRecord);

    ReadBuffer readBuffer = pickler.wrapForReading(writeBuffer.flip());
    ArrayIntRecord deserializedRecord = pickler.deserialize(readBuffer);

    assertArrayEquals(testRecord.integers(), deserializedRecord.integers());
  }

  @Test
  void testLargeLongArray() throws Throwable {
    RecordPickler<ArrayLongRecord> pickler = createPickler(ArrayLongRecord.class);

    Random random = new Random(1234315135L);

    long[] longs = new long[128];
    for (int i = 0; i < longs.length; i++) {
      // random longs will flip a load of bits so be too large for varint encoding
      longs[i] = random.nextLong();
    }

    ArrayLongRecord testRecord = new ArrayLongRecord( longs );

    WriteBuffer writeBuffer = pickler.allocateForWriting(pickler.maxSizeOf (testRecord));
    pickler.serialize(writeBuffer, testRecord);

    ReadBuffer readBuffer = pickler.wrapForReading(writeBuffer.flip());
    ArrayLongRecord deserializedRecord = pickler.deserialize(readBuffer);

    assertArrayEquals(testRecord.longs(), deserializedRecord.longs());
  }

  @Test
  void testLargeVarLongArray() throws Throwable {
    RecordPickler<ArrayLongRecord> pickler = createPickler(ArrayLongRecord.class);

    long[] longs = new long[128];
    for (int i = 0; i < longs.length; i++) {
      longs[i] = i * 1000L; // Write in a way that is better suited for varlong encoding
    }

    ArrayLongRecord testRecord = new ArrayLongRecord( longs );

    WriteBuffer writeBuffer = pickler.allocateForWriting(pickler.maxSizeOf (testRecord));
    pickler.serialize(writeBuffer, testRecord);

    ReadBuffer readBuffer = pickler.wrapForReading(writeBuffer.flip());
    ArrayLongRecord deserializedRecord = pickler.deserialize(readBuffer);

    assertArrayEquals(testRecord.longs(), deserializedRecord.longs());
  }

  public record UUIDRecord(UUID uuid) {
  }

  @Test
  void testUUID() throws Throwable {
    RecordPickler<UUIDRecord> pickler = createPickler(UUIDRecord.class);

    UUID testUUID = new UUID(12343535L, 9876543210L);
    UUIDRecord testRecord = new UUIDRecord(testUUID);

    WriteBuffer writeBuffer = pickler.allocateForWriting(pickler.maxSizeOf (testRecord));
    pickler.serialize(writeBuffer, testRecord);

    ReadBuffer readBuffer = pickler.wrapForReading(writeBuffer.flip());
    UUIDRecord deserialized = pickler.deserialize(readBuffer);

    assertEquals(testUUID, deserialized.uuid());
  }

  public record LinkedRecord(LinkedRecord next, String value) {}

  public record LinkedLong(long someLong, LinkedInt next) {}
  public record LinkedInt(int someInt, LinkedLong next) {}
  
  // Test enum and record containing enum for ENUM serialization tests
  public enum Priority { LOW, MEDIUM, HIGH }
  public record TaskRecord(String name, Priority priority, boolean completed) {}

  @Test
  public void testLinkedRecord() throws Throwable {
    RecordPickler<LinkedRecord> pickler = createPickler(LinkedRecord.class);

    // Create a linked record structure
    LinkedRecord record3 = new LinkedRecord(null, "three");
    LinkedRecord record2 = new LinkedRecord(record3, "two");
    LinkedRecord record1 = new LinkedRecord(record2, "one");

    WriteBuffer writeBuffer = pickler.allocateForWriting(1024); // TODO: fix allocateSufficient size calculation later
    pickler.serialize(writeBuffer, record1);

    ReadBuffer readBuffer = pickler.wrapForReading(writeBuffer.flip());
    LinkedRecord deserialized = pickler.deserialize(readBuffer);

    // Verify the structure
    assertEquals("one", deserialized.value());
    assertNotNull(deserialized.next());
    assertEquals("two", deserialized.next().value());
    assertNotNull(deserialized.next().next());
    assertEquals("three", deserialized.next().next().value());
    assertNull(deserialized.next().next().next());
  }

  @Test
  public void testMutuallyRecursiveRecords() throws Throwable {
    RecordPickler<LinkedLong> pickler = createPickler(LinkedLong.class);

    // Create a mutually recursive structure: Long -> Int -> Long -> Int -> null
    // Use extreme values to force different encoding strategies
    LinkedInt record4 = new LinkedInt(Integer.MIN_VALUE, null); // forces 32-bit int encoding
    LinkedLong record3 = new LinkedLong(Long.MAX_VALUE, record4); // forces 64-bit long encoding  
    LinkedInt record2 = new LinkedInt(42, record3); // small int - uses varint encoding
    LinkedLong record1 = new LinkedLong(1000L, record2); // small long - uses varlong encoding

    WriteBuffer writeBuffer = pickler.allocateForWriting(1024); // TODO: fix allocateSufficient size calculation later
    pickler.serialize(writeBuffer, record1);

    ReadBuffer readBuffer = pickler.wrapForReading(writeBuffer.flip());
    LinkedLong deserialized = pickler.deserialize(readBuffer);

    // Verify the structure and values
    assertEquals(1000L, deserialized.someLong());
    assertNotNull(deserialized.next());
    assertEquals(42, deserialized.next().someInt());
    assertNotNull(deserialized.next().next());
    assertEquals(Long.MAX_VALUE, deserialized.next().next().someLong());
    assertNotNull(deserialized.next().next().next());
    assertEquals(Integer.MIN_VALUE, deserialized.next().next().next().someInt());
    assertNull(deserialized.next().next().next().next());
  }

  @Test
  public void testEnumSerialization() throws Throwable {
    RecordPickler<TaskRecord> pickler = createPickler(TaskRecord.class);

    // Test record with different enum values
    TaskRecord task1 = new TaskRecord("Important Task", Priority.HIGH, false);
    TaskRecord task2 = new TaskRecord("Normal Task", Priority.MEDIUM, true);
    TaskRecord task3 = new TaskRecord("Optional Task", Priority.LOW, false);

    // Test each task
    for (TaskRecord originalTask : List.of(task1, task2, task3)) {
      WriteBuffer writeBuffer = pickler.allocateForWriting(1024);
      pickler.serialize(writeBuffer, originalTask);

      ReadBuffer readBuffer = pickler.wrapForReading(writeBuffer.flip());
      TaskRecord deserializedTask = pickler.deserialize(readBuffer);

      // Verify all fields are correctly deserialized
      assertEquals(originalTask.name(), deserializedTask.name());
      assertEquals(originalTask.priority(), deserializedTask.priority());
      assertEquals(originalTask.completed(), deserializedTask.completed());
      
      // Verify enum identity (enums should be the same instance)
      assertSame(originalTask.priority(), deserializedTask.priority());
    }
  }

  @Test 
  void testPrimitiveWritePerformance() throws Exception {
    // FIXME: NFP write performance regression - 10x slower than JDK on primitives
    // This test ensures we catch performance regressions in primitive serialization
    
    record AllPrimitives(
        boolean boolVal, byte byteVal, short shortVal, char charVal,
        int intVal, long longVal, float floatVal, double doubleVal
    ) {}
    
    final var testData = new AllPrimitives(
        true, (byte)42, (short)1000, 'A', 123456, 9876543210L, 3.14f, 2.71828
    );
    
    final var pickler = createPickler(AllPrimitives.class);
    
    // Measure NFP write performance
    long startTime = System.nanoTime();
    int iterations = 100_000;
    
    for (int i = 0; i < iterations; i++) {
      try (final var writeBuffer = pickler.allocateForWriting(256)) {
        pickler.serialize(writeBuffer, testData);
        writeBuffer.flip();
      }
    }
    
    long nfpDuration = System.nanoTime() - startTime;
    double nfpOpsPerSecond = (double) iterations / (nfpDuration / 1_000_000_000.0);
    
    LOGGER.info(() -> String.format("NFP Write Performance: %.0f ops/s", nfpOpsPerSecond));
    
    // NFP should achieve reasonable write performance (at least 50k ops/s for primitives)
    // Current performance is ~125k ops/s, but should be much higher given read performance
    assertTrue(nfpOpsPerSecond > 50_000, 
               "NFP primitive write performance too low: " + nfpOpsPerSecond + " ops/s");
  }

}
