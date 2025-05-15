package io.github.simbo1905;

import io.github.simbo1905.no.framework.Pickler;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.IntStream;

import static io.github.simbo1905.no.framework.Pickler.getOrCreate;

public class TestNewWorldApi {

  // @formatter:off
  sealed interface Animal permits Mammal, Bird, Alicorn {}
  sealed interface Mammal extends Animal permits Dog, Cat { }
  sealed interface Bird extends Animal permits Eagle, Penguin {}
  record Alicorn(String name, String[] magicPowers) implements Animal, Serializable {
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
  record Dog(String name, int age) implements Mammal, Serializable {}
  record Cat(String name, boolean purrs) implements Mammal, Serializable {}
  record Eagle(double wingspan) implements Bird, Serializable {}
  record Penguin(boolean canSwim) implements Bird, Serializable {}
  // @formatter:on

  record MyRecord(String message, int number) implements Serializable {
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
    ByteBuffer buffer = ByteBuffer.allocate(1024);
    MyRecord record = new MyRecord("Hello", 42);
    Pickler<MyRecord> pickler = Pickler.manufactureRecordPickler(MyRecord.class);
    pickler.serialize(buffer, record);
    buffer.flip(); // Prepare the buffer for reading
    MyRecord deserializedRecord = pickler.deserialize(buffer);
    System.out.println("Deserialized Record: " + deserializedRecord);

    // Create instances
    Dog dog = new Dog("Buddy", 3);
    Dog dog2 = new Dog("Fido", 2);
    Eagle eagle = new Eagle(2.1);
    Penguin penguin = new Penguin(true);
    Alicorn alicorn = new Alicorn("Twilight Sparkle",
        new String[]{"elements of harmony", "wings of a pegasus"});

    // Test 1: Round-trip dog2
    ByteBuffer dogBuffer = ByteBuffer.allocate(1024);
    Pickler<Dog> dogPickler = getOrCreate(Dog.class);
    dogPickler.serialize(dogBuffer, dog2);
    dogBuffer.flip();
    Dog deserializedDog = dogPickler.deserialize(dogBuffer);
    System.out.println("Dog2 round-trip: " + dog2.equals(deserializedDog));

    ByteBuffer animalBuffer = ByteBuffer.allocate(4096);

    // Test 2: Round-trip list of animals
    List<Animal> animals = List.of(dog, dog2, eagle, penguin, alicorn);

    // Get pickler for sealed interface
    Pickler<Animal> animalPickler =
        Pickler.manufactureSealedPickler(Animal.class);

    // Serialize
    animalBuffer.putInt(animals.size());
    for (Animal animal : animals) {
      //animalPickler.forRecord(animal).serialize(animalBuffer, animal);
      animalPickler.serialize(animalBuffer, animal);
    }
    animalBuffer.flip();

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
