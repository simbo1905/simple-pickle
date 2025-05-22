package io.github.simbo1905;

import io.github.simbo1905.no.framework.PackedBuffer;
import io.github.simbo1905.no.framework.Pickler;

import java.util.List;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class Demo4 {
  public static void main(String[] args) {

// Create an array of Person records
    Person[] people = {
        new Person("Alice", 30),
        new Person("Bob", 25),
        new Person("Charlie", 40)
    };

// Calculate size and allocate buffer
    final var buffer = PackedBuffer.allocateSufficient(people);

// Serialize the array
    Pickler.serializeMany(people, buffer);
    var buf = buffer.flip();

// Deserialize the array
    List<?> deserializedPeople = Pickler.deserializeMany(Person.class, buf);

// Verify the array was properly deserialized
    assertEquals(people.length, deserializedPeople.size());

// Use streams to verify each element matches
    IntStream.range(0, people.length)
        .forEach(i -> assertEquals(people[i], deserializedPeople.get(i)));
  }
}
