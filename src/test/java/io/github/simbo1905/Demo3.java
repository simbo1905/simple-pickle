package io.github.simbo1905;

import io.github.simbo1905.no.framework.Pickler;

import java.nio.ByteBuffer;
import java.util.List;

public class Demo3 {

  public static void main(String[] args) {

    final List<ListRecord> outerList = List.of(new ListRecord(List.of("A", "B")), new ListRecord(List.of("X", "Y")));

// Calculate size and allocate buffer. Limitations of generics means we have shallow copy into an array.
    int size = Pickler.sizeOfMany(outerList.toArray(ListRecord[]::new));

    ByteBuffer buffer = ByteBuffer.allocate(size);

// Serialize. Limitations of generics means we have to pass the sealed interface class.
    Pickler.serializeMany(outerList.toArray(ListRecord[]::new), buffer);

// Flip the buffer to prepare for reading
    buffer.flip();

// Deserialize. Limitations of generics means we have to pass the sealed interface class.
    final List<ListRecord> deserialized = Pickler.deserializeMany(ListRecord.class, buffer);
// will throw an exception if you try to modify the list of the deserialized record
    try {
      deserialized.removeFirst();
      throw new AssertionError("should not be reached");
    } catch (UnsupportedOperationException e) {
      // Expected exception, as the list is immutable
      System.out.println("Works As Expected");
    }
// will throw an exception if you try to modify a list inside the deserialized record
    try {
      deserialized.forEach(l -> l.list().removeFirst());
      throw new AssertionError("should not be reached");
    } catch (UnsupportedOperationException e) {
      // Expected exception, as the list is immutable
      System.out.println("Works As Expected");
    }
  }
}
