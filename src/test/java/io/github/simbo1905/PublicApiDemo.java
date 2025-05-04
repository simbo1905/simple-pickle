package io.github.simbo1905;

import io.github.simbo1905.simple_pickle.Pickler;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;


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

  public static void main(String[] args) {
    // Create instances of different Animal implementations
    Dog dog = new Dog("Buddy", 3);
    Dog dog2 = new Dog("Fido", 2);
    Animal eagle = new Eagle(2.1);
    Penguin penguin = new Penguin(true);
    Alicorn alicorn = new Alicorn("Twilight Sparkle", new String[]{"elements of harmony", "wings of a pegasus"});

    List<Animal> animals = List.of(dog, dog2, eagle, penguin, alicorn);

    List<Record[]> animalsByActualType = Arrays.stream(Animal.class.getPermittedSubclasses())
        .flatMap(subclass -> {
          if (subclass.isInterface() && subclass.isSealed()) {
            return Arrays.stream(subclass.getPermittedSubclasses());
          }
          return Stream.of(subclass);
        })
        .map(concreteClass -> animals.stream()
            .filter(concreteClass::isInstance)
            .toArray(size -> (Record[]) java.lang.reflect.Array.newInstance(concreteClass, size)))
        .toList();
    
    int size = animalsByActualType.stream()
        .mapToInt(Pickler::sizeOfMany)
        .sum();

    final var animalsBuffer = ByteBuffer.allocate(size);

    animalsByActualType.forEach(i -> Pickler.serializeMany(i, animalsBuffer));

    // Flip the buffer to prepare for reading
    animalsBuffer.flip();

    final List<Animal> allReturnedAnimals = new ArrayList<>();

//    Pickler2.allPermittedRecordClasses(Animal.class)
//        .forEach(i -> {
//          // Cast to Class<? extends Record> to help with type inference
//          @SuppressWarnings("unchecked")
//          Class<? extends Record> recordClass = (Class<? extends Record>) i;
//
//          // Deserialize the list of animals
//          var returnedAnimals = Pickler.deserializeMany(recordClass, animalsBuffer);
//
//          // Cast the returned list to List<Animal> since we know all records implement Animal
//          @SuppressWarnings("unchecked")
//          List<Animal> animalList = (List<Animal>) (List<?>) returnedAnimals;
//
//          if (animals.containsAll(animalList)) {
//            LOGGER.info(i.getSimpleName() + " serialized and deserialized correctly");
//          } else {
//            throw new AssertionError("¯\\_(ツ)_/¯");
//          }
//          allReturnedAnimals.addAll(animalList);
//        });
//
//
//    assertArrayEquals(animals.toArray(), allReturnedAnimals.toArray(), "Animals should be equal");
//
//    // buffer should be empty
//    if (animalsBuffer.hasRemaining()) {
//      throw new AssertionError("Buffer should be empty after deserialization");
//    }
  }
}

