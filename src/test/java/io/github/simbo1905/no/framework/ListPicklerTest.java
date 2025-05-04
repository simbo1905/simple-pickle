package io.github.simbo1905.no.framework;

import io.github.simbo1905.no.framework.animal.*;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static io.github.simbo1905.no.framework.Pickler.LOGGER;
import static org.junit.jupiter.api.Assertions.*;

public class ListPicklerTest {

  record ListRecord(List<String> list) {
    // Use the canonical constructor to make an immutable copy
    ListRecord {
      list = List.copyOf(list);
    }
  }

  @Test
  void testImmutableLists() {
    // Here we are deliberately passing in a mutable list to the constructor
    final var original = new ListRecord(new ArrayList<>() {{
      add("A");
      add("B");
    }});

    final List<ListRecord> outerList = List.of(original, new ListRecord(List.of("X", "Y")));

    // Calculate size and allocate buffer
    int size = Pickler.sizeOfMany(outerList.toArray(ListRecord[]::new));
    ByteBuffer buffer = ByteBuffer.allocate(size);

    // Serialize
    Pickler.serializeMany(outerList.toArray(ListRecord[]::new), buffer);

    // Flip the buffer to prepare for reading
    buffer.flip();
    // Deserialize
    final List<ListRecord> deserialized = Pickler.deserializeMany(ListRecord.class, buffer);

    // Verify the record counts
    assertEquals(original.list().size(), deserialized.size());
    // Verify immutable list by getting the deserialized list and trying to add into the list we expect an exception
    assertThrows(UnsupportedOperationException.class, deserialized::removeFirst);

    // Verify buffer is fully consumed
    assertEquals(buffer.limit(), buffer.position());
  }

  @Test
  void testNestedLists() {
    // Create a record with nested lists
    record NestedListRecord(List<List<String>> nestedList) {
    }

    // Make the inner lists.
    List<List<String>> nestedList = new ArrayList<>();
    nestedList.add(Arrays.asList("A", "B", "C"));
    nestedList.add(Arrays.asList("D", "E"));

    // The record has mutable inner lists
    NestedListRecord original = new NestedListRecord(nestedList);

    // Get a pickler for the record
    Pickler<NestedListRecord> pickler = Pickler.forRecord(NestedListRecord.class);

    // Calculate size and allocate buffer
    int size = pickler.sizeOf(original);
    ByteBuffer buffer = ByteBuffer.allocate(size);

    // Serialize
    pickler.serialize(original, buffer);
    buffer.flip();

    // Deserialize
    NestedListRecord deserialized = pickler.deserialize(buffer);

    // Verify the nested list structure
    assertEquals(original.nestedList().size(), deserialized.nestedList().size());

    // Verify the inner lists are equal
    IntStream.range(0, original.nestedList().size())
        .forEach(i ->
            assertEquals(original.nestedList().get(i).size(), deserialized.nestedList().get(i).size()));


    // Verify buffer is fully consumed
    assertEquals(buffer.limit(), buffer.position());

    // verify the inner lists are immutable
    assertThrows(UnsupportedOperationException.class, () -> deserialized.nestedList().removeFirst());
  }

  @Test
  void testOuterLists() {
    // Create a record with nested lists
    record NestedListRecord(List<List<String>> nestedList) {
    }

    final List<NestedListRecord> original = new ArrayList<>();

    {
      // Create an instance
      List<List<String>> nestedList = new ArrayList<>();
      nestedList.add(Arrays.asList("A", "B", "C"));
      nestedList.add(Arrays.asList("D", "E"));

      NestedListRecord item = new NestedListRecord(nestedList);
      original.add(item);
    }

    {
      // Create another instance
      List<List<String>> nestedList = new ArrayList<>();
      nestedList.add(List.of("F"));
      nestedList.add(List.of());

      NestedListRecord item = new NestedListRecord(nestedList);
      original.add(item);
    }

    // Calculate size and allocate buffer
    int size = Pickler.sizeOfMany(original.toArray(NestedListRecord[]::new));
    ByteBuffer buffer = ByteBuffer.allocate(size);

    // Serialize
    Pickler.serializeMany(original.toArray(NestedListRecord[]::new), buffer);
    buffer.flip();

    // Deserialize
    List<NestedListRecord> deserialized = Pickler.deserializeMany(NestedListRecord.class, buffer);

    // Verify the nested list structure
    assertEquals(original.size(), deserialized.size());

    IntStream.range(0, original.size()).forEach(i -> {
      assertEquals(original.get(i).nestedList().size(), deserialized.get(i).nestedList().size());
      IntStream.range(0, original.get(i).nestedList().size()).forEach(j -> assertEquals(original.get(i).nestedList().get(j), deserialized.get(i).nestedList().get(j)));
    });

    // Verify buffer is fully consumed
    assertEquals(buffer.limit(), buffer.position());
  }

  @Test
  void demoTests() {
    // Create instances of different Animal implementations
    Dog dog = new Dog("Buddy", 3);
    Dog dog2 = new Dog("Fido", 2);
    Animal eagle = new Eagle(2.1);
    Penguin penguin = new Penguin(true);
    Alicorn alicorn = new Alicorn("Twilight Sparkle", new String[]{"elements of harmony", "wings of a pegasus"});

    // Get a pickler for the sealed trait Animal
    var animalPickler = Pickler.forSealedInterface(Animal.class);

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
      LOGGER.info("Dog serialized and deserialized correctly");
    } else {
      throw new AssertionError("""
          ¯\\_(ツ)_/¯
          """);
    }
    // Check if the number of bytes written is correct
    if (bytesWritten != animalPickler.sizeOf(dog)) {
      throw new AssertionError("wrong number of bytes written");
    }

    List<Animal> animals = List.of(dog, dog2, eagle, penguin, alicorn);

    Dog[] dogs = animals.stream().filter(i -> i instanceof Dog).toArray(Dog[]::new);
    Eagle[] eagles = animals.stream().filter(i -> i instanceof Eagle).toArray(Eagle[]::new);
    Penguin[] penguins = animals.stream().filter(i -> i instanceof Penguin).toArray(Penguin[]::new);
    Alicorn[] alicorns = animals.stream().filter(i -> i instanceof Alicorn).toArray(Alicorn[]::new);

    int size = Stream.of(dogs, eagles, penguins, alicorns)
        .mapToInt(Pickler::sizeOfMany)
        .sum();

    final var animalsBuffer = ByteBuffer.allocate(size);

    // Serialize the list of animals
    Stream.of(dogs, eagles, penguins, alicorns)
        .forEach(i -> Pickler.serializeMany(i, animalsBuffer));

    // Flip the buffer to prepare for reading
    animalsBuffer.flip();

    final List<Animal> allReturnedAnimals = new ArrayList<>();

    Stream.of(Dog.class, Eagle.class, Penguin.class, Alicorn.class)
        .forEach(i -> {
          // Deserialize the list of animals
          //var returnedAnimals = PicklerOld.deserializeArray(i, animalsBuffer);
          var returnedAnimals = Pickler.deserializeMany(i, animalsBuffer);
          // Check if the deserialized Dog instance is equal to the original
          if (animals.containsAll(returnedAnimals)) {
            LOGGER.info(i.getSimpleName() + " serialized and deserialized correctly");
          } else {
            throw new AssertionError("""
                ¯\\_(ツ)_/¯
                """);
          }
          allReturnedAnimals.addAll(returnedAnimals);
        });
    assertArrayEquals(animals.toArray(), allReturnedAnimals.toArray(), "Animals should be equal");
  }
}
