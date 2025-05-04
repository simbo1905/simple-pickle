package io.github.simbo1905;

import io.github.simbo1905.no.framework.Pickler;

import java.nio.ByteBuffer;

public class Demo1 {
  public static void main(String[] args) {
// Create an instance
    var december = new Month(Season.WINTER, "December");

// Get a pickler for the record type containing the enum
    Pickler<Month> pickler = Pickler.forRecord(Month.class);

// Calculate size and allocate buffer
    int size = pickler.sizeOf(december);
    ByteBuffer buffer = ByteBuffer.allocate(size);

// Serialize to a ByteBuffer
    pickler.serialize(december, buffer);
    buffer.flip();

// Deserialize from the ByteBuffer
    Month deserializedMonth = pickler.deserialize(buffer);

    if (deserializedMonth.equals(december)) {
      System.out.println("Works As Expected");
    } else {
      throw new AssertionError("should not be reached");
    }
  }
}
