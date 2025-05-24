package io.github.simbo1905.no.framework;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.logging.*;
import java.util.stream.IntStream;

import static io.github.simbo1905.no.framework.Pickler.LOGGER;
import static org.junit.jupiter.api.Assertions.*;

public class ListPicklerTests {

  @BeforeAll
  static void setupLogging() {
    final var logLevel = System.getProperty("java.util.logging.ConsoleHandler.level", "FINER");
    final Level level = Level.parse(logLevel);

    // Configure the primary LOGGER instance
    LOGGER.setLevel(level);
    // Remove all existing handlers to prevent duplicates if this method is called multiple times
    // or if there are handlers configured by default.
    for (Handler handler : LOGGER.getHandlers()) {
      LOGGER.removeHandler(handler);
    }

    ConsoleHandler consoleHandler = new ConsoleHandler();
    consoleHandler.setLevel(level);

    // Create and set a custom formatter
    Formatter simpleFormatter = new Formatter() {
      @Override
      public String format(LogRecord record) {
        return record.getMessage() + "\n";
      }
    };
    consoleHandler.setFormatter(simpleFormatter);

    LOGGER.addHandler(consoleHandler);

    // Ensure parent handlers are not used to prevent duplicate logging from higher-level loggers
    LOGGER.setUseParentHandlers(false);

    LOGGER.info("Logging initialized at level: " + level);
  }

  record ListRecord(List<String> list) {
    // Use the canonical constructor to make an immutable copy
    ListRecord {
      list = List.copyOf(list);
    }
  }

  @Test
  void testList() throws Exception {
    // Here we are deliberately passing in a mutable list to the constructor
    final var original = new ListRecord(new ArrayList<>() {{
      add("A");
      add("B");
    }});

    final var pickler = Pickler.forRecord(ListRecord.class);
    final byte[] bytes;
    try (var buffer = WriteBuffer.allocateSufficient(original)) {
      final int len = pickler.serialize(buffer, original);
      bytes = new byte[len];
      buffer.flip().put(bytes);
    }

    final ListRecord deserialized = pickler.deserialize(ReadBuffer.wrap(bytes));

    // Verify the record counts
    assertEquals(original.list().size(), deserialized.list().size());
    // Verify immutable list by getting the deserialized list and trying to add into the list we expect an exception
    assertThrows(UnsupportedOperationException.class, () -> deserialized.list().removeFirst());
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
    var buffer = WriteBuffer.allocateSufficient(original);

    // Serialize
    pickler.serialize(buffer, original);
    assertFalse(buffer.hasRemaining());
    var buf = ReadBuffer.wrap(buffer.flip());

    // Deserialize
    NestedListRecord deserialized = pickler.deserialize(buf);

    // Verify the nested list structure
    assertEquals(original.nestedList().size(), deserialized.nestedList().size());

    // Verify the inner lists are equal
    IntStream.range(0, original.nestedList().size())
        .forEach(i ->
            assertEquals(original.nestedList().get(i).size(), deserialized.nestedList().get(i).size()));


    // Verify buffer is fully consumed
    assertFalse(buffer.hasRemaining());

    // verify the inner lists are immutable
    assertThrows(UnsupportedOperationException.class, () -> deserialized.nestedList().removeFirst());
  }

}
