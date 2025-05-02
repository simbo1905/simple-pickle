package io.github.simbo1905;

import io.github.simbo1905.simple_pickle.Pickler;

import java.nio.ByteBuffer;
import java.util.List;

import static io.github.simbo1905.simple_pickle.Pickler.picklerForSealedInterface;
import static io.github.simbo1905.simple_pickle.Pickler.recordsOf;


public class PublicApiDemo {

  // @formatter:off
  sealed interface Animal permits Mammal, Bird, Alicorn {}
  sealed interface Mammal extends Animal permits Dog, Cat { }
  sealed interface Bird extends Animal permits Eagle, Penguin {}
  public record Alicorn(String name, String[] magicPowers) implements Animal {}
  public record Dog(String name, int age) implements Mammal {}
  public record Cat(String name, boolean purrs) implements Mammal {}
  public record Eagle(double wingspan) implements Bird {}
  public record Penguin(boolean canSwim) implements Bird {}
  // @formatter:on

  public static void main(String[] args) {
    // Create instances of different Animal implementations
    Dog dog = new Dog("Buddy", 3);
    Animal eagle = new Eagle(2.1);
    Alicorn alicorn = new Alicorn("Twilight Sparkle", new String[]{"elements of harmony", "wings of a pegasus"});

    // Get a pickler for the sealed trait Animal
    var animalPickler = picklerForSealedInterface(Animal.class);

    // preallocate a buffer for the Dog instance
    var buffer = ByteBuffer.allocate(animalPickler.sizeOf(dog));

    // Serialize and deserialize the Dog instance
    animalPickler.serialize(dog, buffer);
    // Flip the buffer to prepare for reading
    int bytesWritten = buffer.position();
    buffer.flip();
    // Deserialize the Dog instance
    var returnedDog = animalPickler.deserialize(buffer);
    // Check if the deserialized Dog instance is equal to the original
    if (dog.equals(returnedDog)) {
      System.out.println("Dog serialized and deserialized correctly");
    } else {
      throw new AssertionError("""
          ¯\\_(ツ)_/¯
          """);
    }
    // Check if the number of bytes written is correct
    if (bytesWritten != animalPickler.sizeOf(dog)) {
      throw new AssertionError("wrong number of bytes written");
    }

    List<Animal> animals = List.of(dog, eagle, alicorn);

    // sum the size of all the different records
    int size = recordsOf(animals.stream())
        .mapToInt(Pickler::sized)
        .sum();

    final var animalsBuffer = ByteBuffer.allocate(size);

    // Serialize the list of animals
    animals.forEach(i -> {
      animalPickler.serialize(i, animalsBuffer);
    });
  }

}

