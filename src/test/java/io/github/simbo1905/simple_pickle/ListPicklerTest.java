package io.github.simbo1905.simple_pickle;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ListPicklerTest {

  @Test
  void testNestedLists() {
    // Create a record with nested lists
    record NestedListRecord(List<List<String>> nestedList) {
    }

    // Create an instance
    List<List<String>> nestedList = new ArrayList<>();
    nestedList.add(Arrays.asList("A", "B", "C"));
    nestedList.add(Arrays.asList("D", "E"));

    NestedListRecord original = new NestedListRecord(nestedList);

    // Get a pickler for the record
    Pickler<NestedListRecord> pickler = Pickler.picklerForRecord(NestedListRecord.class);

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

    for (int i = 0; i < original.nestedList().size(); i++) {
      assertEquals(original.nestedList().get(i), deserialized.nestedList().get(i));
    }

    // Verify buffer is fully consumed
    assertEquals(buffer.limit(), buffer.position());
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
    int size = Pickler.sizeOfList(original);
    ByteBuffer buffer = ByteBuffer.allocate(size);

    // Serialize
    Pickler.serializeList(original, buffer);
    buffer.flip();

    // Deserialize
    List<NestedListRecord> deserialized = Pickler.deserializeList(buffer, NestedListRecord.class);

    // Verify the nested list structure
    assertEquals(original.size(), deserialized.size());

    IntStream.range(0, original.size()).forEach(i -> {
      assertEquals(original.get(i).nestedList().size(), deserialized.get(i).nestedList().size());
      IntStream.range(0, original.get(i).nestedList().size()).forEach(j -> {
        assertEquals(original.get(i).nestedList().get(j), deserialized.get(i).nestedList().get(j));
      });
    });

    // Verify buffer is fully consumed
    assertEquals(buffer.limit(), buffer.position());
  }

}
