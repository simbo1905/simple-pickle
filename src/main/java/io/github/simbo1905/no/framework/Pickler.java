package io.github.simbo1905.no.framework;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.IntStream;

import static io.github.simbo1905.no.framework.Pickler.getOrCreate;

public interface Pickler<T> {
  java.util.logging.Logger LOGGER = java.util.logging.Logger.getLogger(Pickler.class.getName());

  /// Recursively loads the components reachable through record into the buffer. It always writes out all the components.
  ///
  /// @param buffer The buffer to write into
  /// @param record The record to serialize
  void serialize(ByteBuffer buffer, T record);

  /// Recursively unloads components from the buffer and invokes a constructor following compatibility rules.
  /// @param buffer The buffer to read from
  /// @return The deserialized record
  T deserialize(ByteBuffer buffer);

  ConcurrentHashMap<Class<?>, Pickler<?>> REGISTRY = new ConcurrentHashMap<>();

  // In Pickler interface
  @SuppressWarnings("unchecked")
  static <T> Pickler<T> getOrCreate(Class<T> type) {
    return (Pickler<T>) REGISTRY.computeIfAbsent(type, Pickler::manufactureRecordPickler);
  }

  static <R extends Record> Pickler<R> manufactureRecordPickler(Class<?> recordClass) {
    assert 0 < recordClass.getRecordComponents().length;
    return new RecordPickler<>() {
    };
  }

  static <S> Pickler<S> manufactureSealedPickler(Class<S> sealedClass) {
    return new SealedPickler<>() {
    };
  }

  class SealedPickler<S> implements Pickler<S> {


    @Override
    public S deserialize(ByteBuffer buffer) {
      final var length = buffer.getInt();
      byte[] bytes = new byte[length];
      buffer.get(bytes);
      try (ByteArrayInputStream bin = new ByteArrayInputStream(bytes);
           ObjectInputStream in = new ObjectInputStream(bin)) {
        //noinspection unchecked
        return (S) in.readObject();
      } catch (IOException | ClassNotFoundException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public void serialize(ByteBuffer buffer, S animal) {
      manufactureRecordPickler(animal.getClass()).serialize(buffer, (Record) animal);
    }
  }

  class RecordPickler<R extends Record> implements Pickler<R> {

    @Override
    public void serialize(ByteBuffer buffer, R object) {
      assert 0 < object.getClass().getRecordComponents().length;
      try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
           ObjectOutputStream out = new ObjectOutputStream(baos)) {
        out.writeObject(object);
        out.flush();
        byte[] bytes = baos.toByteArray();
        buffer.putInt(bytes.length);
        buffer.put(bytes);
      } catch (Exception e) {
        throw new RuntimeException("Failed to serialize record", e);
      }
    }

    @Override
    public R deserialize(ByteBuffer buffer) {
      final var length = buffer.getInt();
      byte[] bytes = new byte[length];
      buffer.get(bytes);
      try (ByteArrayInputStream bin = new ByteArrayInputStream(bytes);
           ObjectInputStream in = new ObjectInputStream(bin)) {
        //noinspection unchecked
        return (R) in.readObject();
      } catch (IOException | ClassNotFoundException e) {
        throw new RuntimeException(e);
      }
    }
  }
}

class Companion {

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


  public static void main(String[] args) {
    // Example usage
//    ByteBuffer buffer = ByteBuffer.allocate(1024);
//    MyRecord record = new MyRecord("Hello", 42);
//    Pickler<MyRecord> pickler = Pickler.manufactureRecordPickler(MyRecord.class);
//    pickler.serialize(buffer, record);
//    buffer.flip(); // Prepare the buffer for reading
//    MyRecord deserializedRecord = pickler.deserialize(buffer);
//    System.out.println("Deserialized Record: " + deserializedRecord);

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
