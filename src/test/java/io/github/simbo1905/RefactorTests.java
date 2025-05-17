package io.github.simbo1905;

import io.github.simbo1905.no.framework.PackedBuffer;
import io.github.simbo1905.no.framework.Pickler;
import io.github.simbo1905.no.framework.animal.*;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
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
    PackedBuffer arrayBuffer = Pickler.allocate(1024);
    
    // Act
    arrayPickler.serialize(arrayBuffer, arrayRecord);
    final var arrayBuf = arrayBuffer.flip(); // Prepare the buffer for reading
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
    PackedBuffer buffer = Pickler.allocate(1024);
    
    // Act
    pickler.serialize(buffer, record);
    final var buf = buffer.flip(); // Prepare the buffer for reading
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
    PackedBuffer dogBuffer = Pickler.allocate(1024);
    
    // Act
    dogPickler.serialize(dogBuffer, dog);
    final var buf = dogBuffer.flip();
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
    final var animalPackedBuffer = Pickler.allocate(4096);
    
    // Act - Serialize
    animalPackedBuffer.putInt(animals.size());
    for (Animal animal : animals) {
      animalPickler.serialize(animalPackedBuffer, animal);
    }
    final var animalBuffer = animalPackedBuffer.flip();
    
    // Deserialize
    int size = animalBuffer.getInt();
    List<Animal> deserializedAnimals = new ArrayList<>(size);
    IntStream.range(0, size).forEach(i -> {
      Animal animal = animalPickler.deserialize(animalBuffer);
      deserializedAnimals.add(animal);
    });
    
    // Assert
    assertEquals(animals.size(), deserializedAnimals.size(),
        "Deserialized list should have the same number of elements");
        
    IntStream.range(0, animals.size()).forEach(i -> {
      assertEquals(animals.get(i), deserializedAnimals.get(i),
          "Element at index " + i + " should match after deserialization");
    });
  }
  
  @Test
  void testAlicornWithMagicPowersSerialization() {
    // Arrange
    Alicorn alicorn = new Alicorn("Twilight Sparkle",
        new String[]{"elements of harmony", "wings of a pegasus"});
    
    final Pickler<Animal> animalPickler = Pickler.forSealedInterface(Animal.class);
    final var buffer = Pickler.allocate(1024);
    
    // Act
    animalPickler.serialize(buffer, alicorn);
    final var flippedBuffer = buffer.flip();
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
    final var linkedList = new LinkedListNode(1, new LinkedListNode(2, new LinkedListNode(3)));
    Pickler<LinkedListNode> linkedListPickler = Pickler.forRecord(LinkedListNode.class);
    final var buffer = Pickler.allocate(1024);
    linkedListPickler.serialize(buffer, linkedList);
    final var buf = buffer.flip();
    LinkedListNode deserializedLinkedList = linkedListPickler.deserialize(buf);
    assertEquals(linkedList, deserializedLinkedList,
        "Deserialized linked list should equal the original linked list");
  }

  @Test
  void testTreeNodeSerialization() {
    // Arrange
    final var originalRoot = new TreeNode.InternalNode("Root",
        new TreeNode.InternalNode("Branch1", new TreeNode.LeafNode(42), new TreeNode.LeafNode(99)),
        new TreeNode.InternalNode("Branch2", new TreeNode.LeafNode(123), null));

    Pickler<TreeNode> treeNodePickler = Pickler.forSealedInterface(TreeNode.class);

    // Act - Serialize
    final var buffer = Pickler.allocate(1024);
    treeNodePickler.serialize(buffer, originalRoot);

    // Deserialize
    final var buf = buffer.flip();
    TreeNode deserializedRoot = treeNodePickler.deserialize(buf);

    // Assert
    assertTrue(TreeNode.areTreesEqual(originalRoot, deserializedRoot),
        "Deserialized tree should be equal to the original tree");
  }
}
