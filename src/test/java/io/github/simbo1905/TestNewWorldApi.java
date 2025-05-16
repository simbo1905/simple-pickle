package io.github.simbo1905;

import io.github.simbo1905.no.framework.PackedBuffer;
import io.github.simbo1905.no.framework.Pickler;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.IntStream;

import static io.github.simbo1905.no.framework.Pickler.forRecord;


public class TestNewWorldApi {

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

  public record MyRecord(String message, int number) implements Serializable {
    @Override
    public String toString() {
      return "MyRecord{" +
          "message='" + message + '\'' +
          ", number=" + number +
          '}';
    }
  }

  public static void main(String[] args) {
    // Example usage
    MyRecord record = new MyRecord("Hello", 42);
    Pickler<MyRecord> pickler = forRecord(MyRecord.class);
    PackedBuffer buffer = pickler.allocate(1024);
    pickler.serialize(buffer, record);
    final var buf = buffer.flip(); // Prepare the buffer for reading
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
    PackedBuffer dogBuffer = dogPickler.allocate(1024);
    dogPickler.serialize(dogBuffer, dog2);
    final var buf2 = dogBuffer.flip();
    Dog deserializedDog = dogPickler.deserialize(buf2);
    System.out.println("Dog2 round-trip: " + dog2.equals(deserializedDog));

    // Get pickler for sealed interface
    final Pickler<Animal> animalPickler = Pickler.forSealedInterface(Animal.class);

    final var animalPacedBuffer = animalPickler.allocate(4096);

    // Test 2: Round-trip list of animals
    List<Animal> animals = List.of(dog, dog2, eagle, penguin, alicorn);

    // Serialize
    animalPacedBuffer.putInt(animals.size());
    for (Animal animal : animals) {
      animalPickler.serialize(animalPacedBuffer, animal);
    }
    final var animalBuffer = animalPacedBuffer.flip();

    // Deserialize
    int size = animalBuffer.getInt();
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
