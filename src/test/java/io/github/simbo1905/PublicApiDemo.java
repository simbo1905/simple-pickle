package io.github.simbo1905;

import io.github.simbo1905.no.framework.Pickler;
import io.github.simbo1905.no.framework.ReadBuffer;
import io.github.simbo1905.no.framework.WriteBuffer;
import org.junit.jupiter.api.Assertions;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;


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
    final var buffer = WriteBuffer.of(1024);

    for (Animal animal : animals) {
      pickler.serialize(buffer, animal);
    }

    final var buf = ReadBuffer.wrap(buffer.flip()); // Prepare for reading

    for (Animal animal : animals) {
      Animal deserializedAnimal = pickler.deserialize(buf);
      Assertions.assertEquals(animal, deserializedAnimal);
    }

  }
}

