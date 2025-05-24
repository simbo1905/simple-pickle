package io.github.simbo1905;


import io.github.simbo1905.no.framework.Pickler;
import io.github.simbo1905.no.framework.ReadBuffer;
import io.github.simbo1905.no.framework.WriteBuffer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.IntStream;

public class DemoPickler2 {
  // @formatter:off
  sealed interface Animal permits Mammal, Bird, Alicorn {}
  sealed interface Mammal extends Animal permits Dog, Cat { }
  sealed interface Bird extends Animal permits Eagle, Penguin {}
  public record Alicorn(String name, String[] magicPowers) implements Animal {
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
  public record Dog(String name, int age) implements Mammal {}
  public record Cat(String name, boolean purrs) implements Mammal {}
  public record Eagle(double wingspan) implements Bird {}
  public record Penguin(boolean canSwim) implements Bird {}
  // @formatter:on

  public static void main(String[] args) {
    // Create instances
    Dog dog = new Dog("Buddy", 3);
    Dog dog2 = new Dog("Fido", 2);
    Eagle eagle = new Eagle(2.1);
    Penguin penguin = new Penguin(true);
    Alicorn alicorn = new Alicorn("Twilight Sparkle",
        new String[]{"elements of harmony", "wings of a pegasus"});

    // Test 1: Round-trip dog2
    Pickler<Dog> dogPickler = Pickler.forRecord(Dog.class);
    var dogBuffer = WriteBuffer.of(1024);
    dogPickler.serialize(dogBuffer, dog2);
    final var buf = ReadBuffer.wrap(dogBuffer.flip());
    Dog deserializedDog = dogPickler.deserialize(buf);
    System.out.println("Dog2 round-trip: " + dog2.equals(deserializedDog));


    // Test 2: Round-trip list of animals
    List<Animal> animals = List.of(dog, dog2, eagle, penguin, alicorn);

    // Get pickler for sealed interface
    Pickler<Animal> animalPickler = Pickler.forSealedInterface(Animal.class);

    final var animalBuffer = WriteBuffer.of(4096);

    // Serialize
    animalBuffer.putVarInt(animals.size());

    for (Animal animal : animals) {
      animalPickler.serialize(animalBuffer, animal);
    }
    final var buf2 = ReadBuffer.wrap(animalBuffer.flip());

    // Deserialize
    int size = buf2.getVarInt();
    List<Animal> deserializedAnimals = new ArrayList<>(size);
    IntStream.range(0, size).forEach(i -> {
      Animal animal = animalPickler.deserialize(buf2);
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
