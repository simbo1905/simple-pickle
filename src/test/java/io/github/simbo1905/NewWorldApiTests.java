package io.github.simbo1905;

import io.github.simbo1905.no.framework.Pickler;
import io.github.simbo1905.no.framework.ReadBuffer;
import io.github.simbo1905.no.framework.WriteBuffer;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.logging.*;
import java.util.stream.IntStream;

import static io.github.simbo1905.no.framework.Pickler.LOGGER;
import static io.github.simbo1905.no.framework.Pickler.forRecord;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

/// Tests for serialization and deserialization of various record types
/// using the no-framework API.
class NewWorldApiTests {
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

  // Model definitions
  public sealed interface Animal permits Mammal, Bird, Alicorn {
  }

  public sealed interface Mammal extends Animal permits Dog, Cat {
  }

  public sealed interface Bird extends Animal permits Eagle, Penguin {
  }

  public record Alicorn(String name, String[] magicPowers) implements Animal, Serializable {
    @Override
    public boolean equals(Object o) {
      if (o == null || getClass() != o.getClass()) return false;
      Alicorn alicorn = (Alicorn) o;
      return Objects.equals(name, alicorn.name) && Objects.deepEquals(magicPowers, alicorn.magicPowers);
    }

    @Override
    public int hashCode() {
      return Objects.hash(name, Arrays.hashCode(magicPowers));
    }
  }

  public record Dog(String name, int age) implements Mammal, Serializable {
  }

  public record Cat(String name, boolean purrs) implements Mammal, Serializable {
  }

  public record Eagle(double wingspan) implements Bird, Serializable {
  }

  public record Penguin(boolean canSwim) implements Bird, Serializable {
  }

  public record MyRecord(String message, int number) {
    @Override
    public String toString() {
      return "MyRecord{message='" + message + "', number=" + number + '}';
    }
  }

  public record ArrayRecord(String[] ints) {
  }

  @Test
  void testArrayRecordSerialization() {
    // Given an array record
    final var arrayRecord = new ArrayRecord(new String[]{"a", "b"});
    final var arrayPickler = forRecord(ArrayRecord.class);

    // When serializing and deserializing
    final var writeBuffer = WriteBuffer.of(1024);
    arrayPickler.serialize(writeBuffer, arrayRecord);
    final var readBuffer = ReadBuffer.wrap(writeBuffer.flip());
    final var deserializedArrayRecord = arrayPickler.deserialize(readBuffer);

    // Then the records should be equal
    assertArrayEquals(arrayRecord.ints(), deserializedArrayRecord.ints());
  }

  @Test
  void testDogRoundTripArray() throws Exception {
    // Given a dog instance
    final var dog = new Dog("Fido", 2);
    final var dog2 = new Dog("Scooby", 56);

    final var dogArray = new Dog[]{dog, dog2};

    final var dogPickler = forRecord(Dog.class);

    final byte[] bytes;

    // When serializing and deserializing
    try (final var writeBuffer = WriteBuffer.of(1024)) {
      writeBuffer.putVarInt(dogArray.length);
      for (Dog d : dogArray) {
        dogPickler.serialize(writeBuffer, d);
      }
      final var size = writeBuffer.position();
      bytes = new byte[size];
      writeBuffer.flip().get(bytes);
    }
    final Dog[] deserializedDogs;
    try( final var readBuffer = ReadBuffer.wrap(bytes)) {
      // Read the size of the array
      final var size = readBuffer.getVarInt();
      deserializedDogs = new Dog[size];
      // Deserialize each dog in the array
      Arrays.setAll(deserializedDogs, i -> dogPickler.deserialize(readBuffer));
    }

    assertEquals(dogArray.length, deserializedDogs.length);
    // Then the records should be equal
    assertArrayEquals(dogArray, deserializedDogs);
  }

  @Test
  void testSimpleRecordSerialization() {
    // Given a simple record
    final var record = new MyRecord("Hello", 42);
    final var pickler = forRecord(MyRecord.class);

    // When serializing and deserializing
    final var writeBuffer = WriteBuffer.of(1024);
    pickler.serialize(writeBuffer, record);
    final var readBuffer = ReadBuffer.wrap(writeBuffer.flip());
    final var deserializedRecord = pickler.deserialize(readBuffer);

    // Then the records should be equal
    assertEquals(record, deserializedRecord);
  }

  @Test
  void testDogRoundTrip() {
    // Given a dog instance
    final var dog = new Dog("Fido", 2);
    final var dogPickler = forRecord(Dog.class);

    // When serializing and deserializing
    final var writeBuffer = WriteBuffer.of(1024);
    dogPickler.serialize(writeBuffer, dog);
    final var readBuffer = ReadBuffer.wrap(writeBuffer.flip());
    final var deserializedDog = dogPickler.deserialize(readBuffer);

    // Then the records should be equal
    assertEquals(dog, deserializedDog);
  }

  @Test
  void testAnimalListRoundTrip() throws Exception {
    // Given a list of different animal types
    final var dog = new Dog("Buddy", 3);
    final var dog2 = new Dog("Fido", 2);
    final var eagle = new Eagle(2.1);
    final var penguin = new Penguin(true);
    final var alicorn = new Alicorn("Twilight Sparkle",
        new String[]{"elements of harmony", "wings of a pegasus"});
    final var animals = List.of(dog, dog2, eagle, penguin, alicorn);

    // Get pickler for sealed interface
    final var animalPickler = Pickler.forSealedInterface(Animal.class);
    final var writeBuffer = WriteBuffer.of(4096);

    // When serializing the list
    writeBuffer.putVarInt(animals.size());
    for (Animal animal : animals) {
      animalPickler.serialize(writeBuffer, animal);
    }
    try (final var readBuffer = ReadBuffer.wrap(writeBuffer.flip())) {
      // And deserializing the list
      final var size = readBuffer.getVarInt();
      final var deserializedAnimals = new ArrayList<Animal>(size);
      IntStream.range(0, size).forEach(i -> {
        final var animal = animalPickler.deserialize(readBuffer);
        deserializedAnimals.add(animal);
      });

      // Then all animals should match
      assertEquals(animals.size(), deserializedAnimals.size());
      IntStream.range(0, animals.size()).forEach(i ->
          assertEquals(animals.get(i), deserializedAnimals.get(i)));
    }
  }
}
