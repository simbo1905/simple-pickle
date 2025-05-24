package io.github.simbo1905;

import io.github.simbo1905.no.framework.Pickler;
import io.github.simbo1905.no.framework.ReadBuffer;
import io.github.simbo1905.no.framework.WriteBuffer;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.*;
import java.util.stream.IntStream;

import static io.github.simbo1905.no.framework.Pickler.LOGGER;
import static io.github.simbo1905.no.framework.Pickler.forRecord;

public class TestNewWorldApi {
  static {
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

  // @formatter:off
  public sealed interface Animal permits Mammal, Bird, Alicorn {}
  public sealed interface Mammal extends Animal permits Dog, Cat { }
  public sealed interface Bird extends Animal permits Eagle, Penguin {}
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
  public record Dog(String name, int age) implements Mammal, Serializable {}
  public record Cat(String name, boolean purrs) implements Mammal, Serializable {}
  public record Eagle(double wingspan) implements Bird, Serializable {}
  public record Penguin(boolean canSwim) implements Bird, Serializable {}
  // @formatter:on

  public record MyRecord(String message, int number) {
    @Override
    public String toString() {
      return "MyRecord{" +
          "message='" + message + '\'' +
          ", number=" + number +
          '}';
    }
  }

  public record ArrayRecord(String[] ints) {
  }

  public static void main(String[] args) {

    ArrayRecord arrayRecord = new ArrayRecord(new String[]{"a", "b"});
    Pickler<ArrayRecord> arrayPickler = forRecord(ArrayRecord.class);
    WriteBuffer arrayBuffer = WriteBuffer.of(1024);
    arrayPickler.serialize(arrayBuffer, arrayRecord);
    final var arrayBuf = ReadBuffer.wrap(arrayBuffer.flip()); // Prepare the buffer for reading
    ArrayRecord deserializedArrayRecord = arrayPickler.deserialize(arrayBuf);
    System.out.println("Deserialized ArrayRecord: " + deserializedArrayRecord);
    if (!Arrays.equals(arrayRecord.ints(), deserializedArrayRecord.ints())) {
      throw new RuntimeException("Deserialization failed");
    }

    // Example usage
    MyRecord record = new MyRecord("Hello", 42);
    Pickler<MyRecord> pickler = forRecord(MyRecord.class);
    WriteBuffer buffer = WriteBuffer.of(1024);
    pickler.serialize(buffer, record);
    final var buf = ReadBuffer.wrap(buffer.flip()); // Prepare the buffer for reading
    MyRecord deserializedRecord = pickler.deserialize(buf);
    System.out.println("Deserialized Record: " + deserializedRecord);
    if (!record.equals(deserializedRecord)) {
      throw new RuntimeException("Deserialization failed");
    }

    // Create instances
    Dog dog = new Dog("Buddy", 3);
    Dog dog2 = new Dog("Fido", 2);
    Eagle eagle = new Eagle(2.1);
    Penguin penguin = new Penguin(true);
    Alicorn alicorn = new Alicorn("Twilight Sparkle",
        new String[]{"elements of harmony", "wings of a pegasus"});

    // Test 1: Round-trip dog2
    Pickler<Dog> dogPickler = forRecord(Dog.class);
    WriteBuffer dogBuffer = WriteBuffer.of(1024);
    dogPickler.serialize(dogBuffer, dog2);
    final var buf2 = ReadBuffer.wrap(dogBuffer.flip());
    Dog deserializedDog = dogPickler.deserialize(buf2);
    System.out.println("Dog2 round-trip: " + dog2.equals(deserializedDog));

    // Get pickler for sealed interface
    final Pickler<Animal> animalPickler = Pickler.forSealedInterface(Animal.class);

    final var animalPacedBuffer = WriteBuffer.of(4096);

    // Test 2: Round-trip list of animals
    List<Animal> animals = List.of(dog, dog2, eagle, penguin, alicorn);

    // Serialize
    animalPacedBuffer.putVarInt(animals.size());
    for (Animal animal : animals) {
      animalPickler.serialize(animalPacedBuffer, animal);
    }
    final var animalBuffer = ReadBuffer.wrap(animalPacedBuffer.flip());

    // Deserialize
    int size = animalBuffer.getVarInt();
    List<Animal> deserializedAnimals = new ArrayList<>(size);
    IntStream.range(0, size).forEach(i -> {
      Animal animal = animalPickler.deserialize(animalBuffer);
      deserializedAnimals.add(animal);
    });

    // Verify
    AtomicBoolean allMatch = new AtomicBoolean(true);
    IntStream.range(0, animals.size()).forEach(i -> {
      if (!animals.get(i).equals(deserializedAnimals.get(i))) {
        allMatch.set(false);
      }
    });
    System.out.println("Animal list round-trip: " + allMatch.get());
  }
}
