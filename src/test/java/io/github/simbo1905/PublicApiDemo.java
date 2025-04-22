package io.github.simbo1905;

import java.nio.ByteBuffer;
import java.util.Arrays;

import static io.github.simbo1905.simple_pickle.Pickler.picklerForSealedTrait;


public class PublicApiDemo {

  // @formatter:off
  sealed interface Animal permits Mammal, Bird, Alicorn {}
  sealed interface Mammal extends Animal permits Dog, Cat { }
  sealed interface Bird extends Animal permits Eagle, Penguin {}
  public record Alicorn(String name, String[] magicPowers) implements Animal {}
  public record Dog(String name, int age) implements Mammal {}
  public record Cat(String name, boolean purrs) implements Mammal {}
  public record Eagle(double wingspan) implements Bird {}
  record Penguin(boolean canSwim) implements Bird {}
  // @formatter:on

  public static void main(String[] args) {
    // Create instances of different Animal implementations
    Dog dog = new Dog("Buddy", 3);
    Animal eagle = new Eagle(2.1);
    Alicorn alicorn = new Alicorn("Twilight Sparkle", new String[]{"elements of harmony", "wings of a pegasus"});

    // Get a pickler for the sealed trait Animal
    var animalPickler = picklerForSealedTrait(Animal.class);

    // Serialize and deserialize the Dog instance
    var dogBuffer = ByteBuffer.allocate(64);
    animalPickler.serialize(dog, dogBuffer);
    int bytesWritten = dogBuffer.position();
    dogBuffer.flip();
    var returnedDog = animalPickler.deserialize(dogBuffer);

    if (dog.equals(returnedDog)) {
      System.out.println("Dog serialized and deserialized correctly");
    } else {
      throw new AssertionError("""
          ¯\\_(ツ)_/¯
          """);
    }

    if (bytesWritten != animalPickler.sizeOf(dog)) {
      throw new AssertionError("wrong number of bytes written");
    }

    // Demo Eagle serialization/deserialization
    var eagleBuffer = ByteBuffer.allocate(64);
    animalPickler.serialize(eagle, eagleBuffer);
    eagleBuffer.flip();
    var returnedEagle = animalPickler.deserialize(eagleBuffer);

    if (eagle.equals(returnedEagle)) {
      System.out.println("Eagle serialized and deserialized correctly");
    } else {
      throw new AssertionError("Eagle serialization failed");
    }

    // Demo Alicorn serialization/deserialization
    var alicornBuffer = ByteBuffer.allocate(256);
    animalPickler.serialize(alicorn, alicornBuffer);
    alicornBuffer.flip();
    var returnedAlicorn = (Alicorn) animalPickler.deserialize(alicornBuffer);

    if (Arrays.equals(alicorn.magicPowers(), returnedAlicorn.magicPowers())) {
      System.out.println("Alicorn serialized and deserialized correctly");
    } else {
      throw new AssertionError("Alicorn serialization failed");
    }
  }
}

