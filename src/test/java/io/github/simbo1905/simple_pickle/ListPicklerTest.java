package io.github.simbo1905.simple_pickle;

import io.github.simbo1905.simple_pickle.protocol.Push;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

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
    int size = Pickler.sizeOfList(ListRecord.class, outerList);
    ByteBuffer buffer = ByteBuffer.allocate(size);

    // Serialize
    Pickler.serializeList(ListRecord.class, outerList, buffer);
    // Flip the buffer to prepare for reading
    buffer.flip();
    // Deserialize
    final List<ListRecord> deserialized = Pickler.deserializeList(ListRecord.class, buffer);

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
    int size = Pickler.sizeOfList(NestedListRecord.class, original);
    ByteBuffer buffer = ByteBuffer.allocate(size);

    // Serialize
    Pickler.serializeList(NestedListRecord.class, original, buffer);
    buffer.flip();

    // Deserialize
    List<NestedListRecord> deserialized = Pickler.deserializeList(NestedListRecord.class, buffer);

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

  @Test
  void sealedInterfacesLists() {
    final List<Push> original = List.of(
        new Push("hello"),
        new Push("hello"),
        new Push("hello"),
        new Push("hello"));
    int size = Pickler.sizeOfList(Push.class, original);
    ByteBuffer buffer = ByteBuffer.allocate(size);
    Pickler.serializeList(Push.class, original, buffer);

  }

}
