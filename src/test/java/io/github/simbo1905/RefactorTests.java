package io.github.simbo1905;

import io.github.simbo1905.no.framework.Pickler;
import io.github.simbo1905.no.framework.ReadBuffer;
import io.github.simbo1905.no.framework.WriteBuffer;
import io.github.simbo1905.no.framework.animal.*;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.logging.*;
import java.util.stream.IntStream;

import static io.github.simbo1905.no.framework.Pickler.LOGGER;
import static io.github.simbo1905.no.framework.Pickler.forRecord;
import static org.junit.jupiter.api.Assertions.*;

public class RefactorTests {

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
  void testArrayRecordSerialization() {
    // Arrange
    TestNewWorldApi.ArrayRecord arrayRecord = new TestNewWorldApi.ArrayRecord(new String[]{"a", "b"});
    Pickler<TestNewWorldApi.ArrayRecord> arrayPickler = forRecord(TestNewWorldApi.ArrayRecord.class);
    WriteBuffer arrayBuffer = WriteBuffer.of(1024);
    
    // Act
    arrayPickler.serialize(arrayBuffer, arrayRecord);
    final var arrayBuf = ReadBuffer.wrap(arrayBuffer.flip()); // Prepare the buffer for reading
    TestNewWorldApi.ArrayRecord deserializedArrayRecord = arrayPickler.deserialize(arrayBuf);
    
    // Assert
    assertArrayEquals(arrayRecord.ints(), deserializedArrayRecord.ints(), 
        "Array fields should match after deserialization");
  }

  @Test
  void testMyRecordSerialization() {
    // Arrange
    TestNewWorldApi.MyRecord record = new TestNewWorldApi.MyRecord("Hello", 42);
    Pickler<TestNewWorldApi.MyRecord> pickler = forRecord(TestNewWorldApi.MyRecord.class);
    WriteBuffer buffer = WriteBuffer.of(1024);
    
    // Act
    pickler.serialize(buffer, record);
    final var buf = ReadBuffer.wrap(buffer.flip()); // Prepare the buffer for reading
    TestNewWorldApi.MyRecord deserializedRecord = pickler.deserialize(buf);
    
    // Assert
    assertEquals(record, deserializedRecord, 
        "Deserialized record should equal the original record");
  }

  @Test
  void testDogSerialization() {
    // Arrange
    Dog dog = new Dog("Fido", 2);
    Pickler<Dog> dogPickler = forRecord(Dog.class);
    // dogPickler.sizeOf(dog)
    WriteBuffer dogBuffer = WriteBuffer.of(1024);
    
    // Act
    dogPickler.serialize(dogBuffer, dog);
    final var buf = ReadBuffer.wrap(dogBuffer.flip());
    Dog deserializedDog = dogPickler.deserialize(buf);
    
    // Assert
    assertEquals(dog, deserializedDog, 
        "Deserialized dog should equal the original dog");
  }

  @Test
  void testAnimalListSerialization() {
    // Arrange
    Dog dog = new Dog("Buddy", 3);
    Dog dog2 = new Dog("Fido", 2);
    Eagle eagle = new Eagle(2.1);
    Penguin penguin = new Penguin(true);
    Alicorn alicorn = new Alicorn("Twilight Sparkle",
        new String[]{"elements of harmony", "wings of a pegasus"});

    List<Animal> animals = List.of(dog, dog2, eagle, penguin, alicorn);
    
    final Pickler<Animal> animalPickler = Pickler.forSealedInterface(Animal.class);
    final var animalPackedBuffer = WriteBuffer.of(4096);
    
    // Act - Serialize
    animalPackedBuffer.putVarInt(animals.size());
    for (Animal animal : animals) {
      animalPickler.serialize(animalPackedBuffer, animal);
    }
    final var animalBuffer = ReadBuffer.wrap(animalPackedBuffer.flip());
    
    // Deserialize
    int size = animalBuffer.getVarInt();
    List<Animal> deserializedAnimals = new ArrayList<>(size);
    IntStream.range(0, size).forEach(i -> {
      Animal animal = animalPickler.deserialize(animalBuffer);
      deserializedAnimals.add(animal);
    });
    
    // Assert
    assertEquals(animals.size(), deserializedAnimals.size(),
        "Deserialized list should have the same number of elements");

    IntStream.range(0, animals.size()).forEach(i -> assertEquals(animals.get(i), deserializedAnimals.get(i),
        "Element at index " + i + " should match after deserialization"));
  }
  
  @Test
  void testAlicornWithMagicPowersSerialization() {
    // Arrange
    Alicorn alicorn = new Alicorn("Twilight Sparkle",
        new String[]{"elements of harmony", "wings of a pegasus"});
    
    final Pickler<Animal> animalPickler = Pickler.forSealedInterface(Animal.class);
    final var buffer = WriteBuffer.of(1024);
    
    // Act
    animalPickler.serialize(buffer, alicorn);
    final var flippedBuffer = ReadBuffer.wrap(buffer.flip());
    Animal deserializedAnimal = animalPickler.deserialize(flippedBuffer);
    
    // Assert
    assertInstanceOf(Alicorn.class, deserializedAnimal, "Deserialized animal should be an instance of Alicorn");
    assertEquals(alicorn, deserializedAnimal,
        "Deserialized alicorn should equal the original alicorn");
  }

  sealed interface TreeNode permits TreeNode.InternalNode, TreeNode.LeafNode {
    record LeafNode(int value) implements TreeNode {
    }

    record InternalNode(String name, TreeNode left, TreeNode right) implements TreeNode {
    }

    /// Sealed interfaces are exhaustively matched within matched pattern matching switch expressions
    static boolean areTreesEqual(TreeNode l, TreeNode r) {
      return switch (l) {
        case null -> r == null;
        case TreeNode.LeafNode(var v1) -> r instanceof TreeNode.LeafNode(var v2) && v1 == v2;
        case TreeNode.InternalNode(String n1, TreeNode i1, TreeNode i2) ->
            r instanceof TreeNode.InternalNode(String n2, TreeNode j1, TreeNode j2) &&
                n1.equals(n2) &&
                areTreesEqual(i1, j1) &&
                areTreesEqual(i2, j2);
      };
    }
  }

  public record LinkedListNode(int value, LinkedListNode next) {
    public LinkedListNode(int value) {
      this(value, null);
    }
  }

  @Test
  void testLinkedNode(){
    LOGGER.info("Starting testLinkedNode");
    final var linkedList = new LinkedListNode(1, new LinkedListNode(2, new LinkedListNode(3)));
    Pickler<LinkedListNode> linkedListPickler = Pickler.forRecord(LinkedListNode.class);
    final var buffer = WriteBuffer.of(1024);
    linkedListPickler.serialize(buffer, linkedList);
    final var buf = ReadBuffer.wrap(buffer.flip());
    LinkedListNode deserializedLinkedList = linkedListPickler.deserialize(buf);
    assertEquals(linkedList, deserializedLinkedList,
        "Deserialized linked list should equal the original linked list");
  }

  @Test
  void testTreeNodeSerialization() {
    LOGGER.info("Starting testTreeNodeSerialization");
    // Arrange
    final var originalRoot = new TreeNode.InternalNode("Root",
        new TreeNode.InternalNode("Branch1", new TreeNode.LeafNode(42), new TreeNode.LeafNode(99)),
        new TreeNode.InternalNode("Branch2", new TreeNode.LeafNode(123), null));

    Pickler<TreeNode> treeNodePickler = Pickler.forSealedInterface(TreeNode.class);

    // Act - Serialize
    final var buffer = WriteBuffer.of(1024);
    treeNodePickler.serialize(buffer, originalRoot);

    // Deserialize
    final var buf = ReadBuffer.wrap(buffer.flip());
    TreeNode deserializedRoot = treeNodePickler.deserialize(buf);

    // Assert
    assertTrue(TreeNode.areTreesEqual(originalRoot, deserializedRoot),
        "Deserialized tree should be equal to the original tree");
  }


  // Test record for byte
  public record ByteRecord(byte value) {
  }

  @Test
  void testByteRecordSerialization() {
    // Arrange
    ByteRecord record = new ByteRecord((byte) 127);
    Pickler<ByteRecord> pickler = forRecord(ByteRecord.class);
    WriteBuffer buffer = WriteBuffer.allocateSufficient(record);

    // Act
    pickler.serialize(buffer, record);
    final var buf = ReadBuffer.wrap(buffer.flip());
    ByteRecord deserialized = pickler.deserialize(buf);

    // Assert
    assertEquals(record, deserialized, "Byte record should be preserved");
  }

  // Test record for short
  public record ShortRecord(short value) {
  }

  @Test
  void testShortRecordSerialization() {
    // Arrange
    ShortRecord record = new ShortRecord((short) 32767);
    Pickler<ShortRecord> pickler = forRecord(ShortRecord.class);
    WriteBuffer buffer = WriteBuffer.allocateSufficient(record);

    // Act
    pickler.serialize(buffer, record);
    final var buf = ReadBuffer.wrap(buffer.flip());
    ShortRecord deserialized = pickler.deserialize(buf);

    // Assert
    assertEquals(record, deserialized, "Short record should be preserved");
  }

  // Test record for float
  public record FloatRecord(float value) {
  }

  @Test
  void testFloatRecordSerialization() {
    // Arrange
    FloatRecord record = new FloatRecord(3.14159f);
    Pickler<FloatRecord> pickler = forRecord(FloatRecord.class);
    WriteBuffer buffer = WriteBuffer.allocateSufficient(record);

    // Act
    pickler.serialize(buffer, record);
    final var buf = ReadBuffer.wrap(buffer.flip());
    FloatRecord deserialized = pickler.deserialize(buf);

    // Assert
    assertEquals(record, deserialized, "Float record should be preserved");
  }

  // Test record for double
  public record DoubleRecord(double value) {
  }

  @Test
  void testDoubleRecordSerialization() {
    // Arrange
    DoubleRecord record = new DoubleRecord(Math.PI);
    Pickler<DoubleRecord> pickler = forRecord(DoubleRecord.class);
    WriteBuffer buffer = WriteBuffer.allocateSufficient(record);

    // Act
    pickler.serialize(buffer, record);
    final var buf = ReadBuffer.wrap(buffer.flip());
    DoubleRecord deserialized = pickler.deserialize(buf);

    // Assert
    assertEquals(record, deserialized, "Double record should be preserved");
  }

  // Test record for Optional<Byte>
  public record OptionalByteRecord(Optional<Byte> value) {
  }

  @Test
  void testOptionalByteRecordSerialization() {
    // Arrange
    OptionalByteRecord record = new OptionalByteRecord(Optional.of((byte) 42));
    Pickler<OptionalByteRecord> pickler = forRecord(OptionalByteRecord.class);
    WriteBuffer buffer = WriteBuffer.allocateSufficient(record);

    // Act - Present value
    pickler.serialize(buffer, record);
    final var buf = ReadBuffer.wrap(buffer.flip());
    OptionalByteRecord deserialized = pickler.deserialize(buf);

    // Assert
    assertEquals(record, deserialized, "Optional<Byte> record with value should be preserved");

    // Test empty optional
    buffer = WriteBuffer.allocateSufficient(record);
    OptionalByteRecord emptyRecord = new OptionalByteRecord(Optional.empty());
    pickler.serialize(buffer, emptyRecord);
    final var buf2 = ReadBuffer.wrap(buffer.flip());
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
    WriteBuffer buffer = WriteBuffer.allocateSufficient(record);

    // Act
    pickler.serialize(buffer, record);
    final var buf = ReadBuffer.wrap(buffer.flip());
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
    WriteBuffer buffer = WriteBuffer.allocateSufficient(record);

    // Act
    pickler.serialize(buffer, record);
    final var buf = ReadBuffer.wrap(buffer.flip());
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
    WriteBuffer buffer = WriteBuffer.allocateSufficient(record);

    // Act
    pickler.serialize(buffer, record);
    final var buf = ReadBuffer.wrap(buffer.flip());
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
    DoubleArrayRecord record = new DoubleArrayRecord(new double[]{Math.PI, Math.E, 1.0 / 3.0, Double.MIN_VALUE, Double.MAX_VALUE});
    Pickler<DoubleArrayRecord> pickler = forRecord(DoubleArrayRecord.class);
    WriteBuffer buffer = WriteBuffer.allocateSufficient(record);

    // Act
    pickler.serialize(buffer, record);
    final var buf = ReadBuffer.wrap(buffer.flip());
    DoubleArrayRecord deserialized = pickler.deserialize(buf);

    // Assert
    assertEquals(record, deserialized, "Double array record should be preserved");
    assertArrayEquals(record.values(), deserialized.values(), "Double array contents should match");
  }
}
