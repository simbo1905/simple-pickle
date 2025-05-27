package io.github.simbo1905.no.framework;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
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

  public record DeepDouble(List<List<Optional<Double>>> deepDoubles){}

  @Test
  void testDeepDoubleList() {
    // Create a record with a deep list of optional doubles
    DeepDouble original = new DeepDouble(
        List.of(
            List.of(Optional.of(1.0), Optional.empty(), Optional.of(3.0)),
            List.of(Optional.empty(), Optional.of(5.0))
        )
    );

    // Get a pickler for the record
    Pickler<DeepDouble> pickler = Pickler.forRecord(DeepDouble.class);

    // Calculate size and allocate buffer
    var buffer = pickler.allocate(1024);

    // Serialize
    pickler.serialize(buffer, original);

    // Flip the buffer to prepare for reading
    var buf = ReadBuffer.wrap(buffer.flip());

    // Deserialize
    DeepDouble deserialized = pickler.deserialize(buf);

    // Verify the deep list structure
    assertEquals(original.deepDoubles().size(), deserialized.deepDoubles().size());
    IntStream.range(0, original.deepDoubles().size())
        .forEach(i -> assertEquals(original.deepDoubles().get(i).size(), deserialized.deepDoubles().get(i).size()));

    // Verify buffer is fully consumed
    assertFalse(buffer.hasRemaining());
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
    var buffer = pickler.allocate(1024);

    // Serialize
    pickler.serialize(buffer, original);

    // Flip the buffer to prepare for reading
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
