package io.github.simbo1905;

import io.github.simbo1905.no.framework.Pickler;
import org.junit.jupiter.api.Assertions;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.IntStream;


public class PublicApiDemo {

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

  // Create instances of different Animal implementations
  static Dog dog = new Dog("Buddy", 3);
  static Dog dog2 = new Dog("Fido", 2);
  static Animal eagle = new Eagle(2.1);
  static Penguin penguin = new Penguin(true);
  static Alicorn alicorn = new Alicorn("Twilight Sparkle", new String[]{"elements of harmony", "wings of a pegasus"});

  static List<Animal> animals = List.of(dog, dog2, eagle, penguin, alicorn);

  public static void main(String[] args) {
    Pickler<Animal> pickler = Pickler.forSealedInterface(Animal.class);
    final var buffer = ByteBuffer.allocate(1024);

    for (Animal animal : animals) {
      pickler.serialize(animal, buffer);
    }

    buffer.flip(); // Prepare for reading

    for (Animal animal : animals) {
      Animal deserializedAnimal = pickler.deserialize(buffer);
      Assertions.assertEquals(animal, deserializedAnimal);
    }

    listsCannotBeTypeOfSealedInterfaceAndPermittedRecords();
  }

  private static void listsCannotBeTypeOfSealedInterfaceAndPermittedRecords() {
    final var buffer = ByteBuffer.allocate(1024);


    Dog[] dogs = animals.stream()
        .filter(Dog.class::isInstance)
        .map(Dog.class::cast)
        .toArray(Dog[]::new);

    Pickler.serializeMany(dogs, buffer);

    Cat[] cats = animals.stream()
        .filter(Cat.class::isInstance)
        .map(Cat.class::cast)
        .toArray(Cat[]::new);

    Pickler.serializeMany(cats, buffer);

    Eagle[] eagles = animals.stream()
        .filter(Eagle.class::isInstance)
        .map(Eagle.class::cast)
        .toArray(Eagle[]::new);

    Pickler.serializeMany(eagles, buffer);

    Penguin[] penguins = animals.stream()
        .filter(Penguin.class::isInstance)
        .map(Penguin.class::cast)
        .toArray(Penguin[]::new);

    Pickler.serializeMany(penguins, buffer);

    Alicorn[] alicorns = animals.stream()
        .filter(Alicorn.class::isInstance)
        .map(Alicorn.class::cast)
        .toArray(Alicorn[]::new);

    Pickler.serializeMany(alicorns, buffer);

    // Deserialize the data back into objects
    buffer.flip(); // Prepare for reading
    Dog[] deserializedDogs = Pickler.deserializeMany(Dog.class, buffer).toArray(Dog[]::new);
    Cat[] deserializedCats = Pickler.deserializeMany(Cat.class, buffer).toArray(Cat[]::new);
    Eagle[] deserializedEagles = Pickler.deserializeMany(Eagle.class, buffer).toArray(Eagle[]::new);
    Penguin[] deserializedPenguins = Pickler.deserializeMany(Penguin.class, buffer).toArray(Penguin[]::new);
    Alicorn[] deserializedAlicorns = Pickler.deserializeMany(Alicorn.class, buffer).toArray(Alicorn[]::new);

    IntStream.range(0, dogs.length)
        .forEach(i -> Assertions.assertEquals(dogs[i], deserializedDogs[i]));
    IntStream.range(0, cats.length)
        .forEach(i -> Assertions.assertEquals(cats[i], deserializedCats[i]));
    IntStream.range(0, eagles.length)
        .forEach(i -> Assertions.assertEquals(eagles[i], deserializedEagles[i]));
    IntStream.range(0, penguins.length)
        .forEach(i -> Assertions.assertEquals(penguins[i], deserializedPenguins[i]));
    IntStream.range(0, alicorns.length)
        .forEach(i -> Assertions.assertEquals(alicorns[i], deserializedAlicorns[i]));
  }
}

