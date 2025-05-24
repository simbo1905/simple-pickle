// SPDX-FileCopyrightText: 2025 Simon Massey
// SPDX-License-Identifier: MIT
package io.github.simbo1905.no.framework;

import io.github.simbo1905.no.framework.animal.*;
import io.github.simbo1905.no.framework.model.*;
import io.github.simbo1905.no.framework.protocol.*;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.logging.*;
import java.util.stream.IntStream;

import static io.github.simbo1905.no.framework.Pickler.LOGGER;
import static org.junit.jupiter.api.Assertions.*;

/// Test class for the Pickler functionality.
/// Demonstrates basic serialization and deserialization of records.
public class PicklerTests {
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

  /**
   * Utility method to check nested array record equality
   * @param expected The expected nested array record
   * @param actual The actual nested array record
   */
  private void assertNestedArrayRecordEquals(NestedArrayExample expected, NestedArrayExample actual) {
    assertEquals(expected.nestedIntArray().length, actual.nestedIntArray().length);
    java.util.stream.IntStream.range(0, expected.nestedIntArray().length)
        .forEach(i -> assertArrayEquals(expected.nestedIntArray()[i], actual.nestedIntArray()[i]));

    assertEquals(expected.nestedObjectArray().length, actual.nestedObjectArray().length);
    java.util.stream.IntStream.range(0, expected.nestedObjectArray().length)
        .forEach(i -> assertDeepArrayEquals(expected.nestedObjectArray()[i], actual.nestedObjectArray()[i]));
  }

  /**
   * Utility method for deep array comparison that handles objects properly
   * @param expected Expected object array
   * @param actual Actual object array
   */
  void assertDeepArrayEquals(Object[] expected, Object[] actual) {
    assertEquals(expected.length, actual.length);
    java.util.stream.IntStream.range(0, expected.length)
        .forEach(i -> {
          if (expected[i] == null) {
            assertNull(actual[i]);
          } else if (expected[i].getClass().isArray()) {
            // Handle nested arrays
            assertTrue(actual[i].getClass().isArray());
            assertDeepArrayEquals((Object[]) expected[i], (Object[]) actual[i]);
          } else {
            assertEquals(expected[i], actual[i]);
          }
        });
  }

  @Test
  void testNestedRecord() {
    // Create nested records
    Person person = new Person("John Doe", 35);
    Address address = new Address("123 Main St", "Any Town", "12345");
    Employee employee = new Employee("E12345", person, address);

    // Get a pickler for the Employee record
    Pickler<Employee> pickler = Pickler.forRecord(Employee.class);

    // Serialize the record with nested records
    final var buffer = WriteBuffer.allocateSufficient(employee);
    pickler.serialize(buffer, employee);

    int bytesWritten = buffer.position();
    var buf = ReadBuffer.wrap(buffer.flip()); // Prepare buffer for reading

    // Deserialize from the byte buffer
    final var deserialized = pickler.deserialize(buf);

    // Verify the deserialized object matches the original
    assertEquals(employee, deserialized);
    assertEquals("E12345", deserialized.id());
    assertEquals(person, deserialized.person());
    assertEquals("John Doe", deserialized.person().name());
    assertEquals(35, deserialized.person().age());
    assertEquals(address, deserialized.address());
    assertEquals("123 Main St", deserialized.address().street());
    assertEquals("Any Town", deserialized.address().city());
    assertEquals("12345", deserialized.address().zipCode());
  }

  @Test
  void testMultiLevelNestedRecord() {
    // Create nested records with multiple levels
    Person person1 = new Person("John Manager", 45);
    Address address1 = new Address("555 Boss Ave", "Manager Town", "54321");
    Employee manager = new Employee("M98765", person1, address1);

    Person person2 = new Person("Jane Employee", 30);
    Address address2 = new Address("123 Work St", "Employee Ville", "12345");
    Employee employee = new Employee("E12345", person2, address2);

    Department department = new Department("Engineering", manager, employee);

    // Get a pickler for the Department record
    Pickler<Department> pickler = Pickler.forRecord(Department.class);

    // Serialize the record with nested records
    final var buffer = WriteBuffer.allocateSufficient(department);
    pickler.serialize(buffer, department);

    var buf = ReadBuffer.wrap(buffer.flip()); // Prepare buffer for reading

    // Deserialize from the byte buffer
    final var deserialized = pickler.deserialize(buf);

    // Verify the deserialized object matches the original
    assertEquals(department, deserialized);
    assertEquals("Engineering", deserialized.name());
    assertEquals(manager, deserialized.manager());
    assertEquals("John Manager", deserialized.manager().person().name());
    assertEquals(employee, deserialized.employee());
    assertEquals("Jane Employee", deserialized.employee().person().name());
  }

  @Test
  void testNestedArrays() {
    // Create a record with nested arrays
    final var original = new NestedArrayExample(
        new int[][]{
            new int[]{1, 2, 3},
            new int[]{4, 5},
            new int[]{6}
        },
        new Object[][]{
            new Object[]{"a", 1, true},
            new Object[]{new Person("Charlie", 40), null}
        }
    );

    Pickler<NestedArrayExample> pickler = Pickler.forRecord(NestedArrayExample.class);

    // Serialize the record
    final var buffer = WriteBuffer.of(1024);
    pickler.serialize(buffer, original);

    var buf = ReadBuffer.wrap(buffer.flip()); // Prepare buffer for reading

    // Deserialize from the byte buffer
    final var deserialized = pickler.deserialize(buf);

    // Replace direct equality check with component-by-component nested array comparison
    assertNestedArrayRecordEquals(original, deserialized);
  }

  @Test
  void testSealedInterface() {
    // Create instances of different Shape implementations
    Shape circle = new Circle(5.0);
    Shape rectangle = new Rectangle(4.0, 6.0);
    Shape triangle = new Triangle(3.0, 4.0, 5.0);

    // Get a pickler for the Shape sealed interface
    Pickler<Shape> pickler = Pickler.forSealedInterface(Shape.class);

    // Test circle
    final var circleBuffer = WriteBuffer.of(1024);
    pickler.serialize(circleBuffer, circle);
    assertFalse(circleBuffer.hasRemaining());
    var buf = ReadBuffer.wrap(circleBuffer.flip());
    Shape deserializedCircle = pickler.deserialize(buf);

    assertInstanceOf(Circle.class, deserializedCircle);
    assertEquals(circle, deserializedCircle);
    assertEquals(5.0, ((Circle) deserializedCircle).radius());

    // Test rectangle
    final var rectangleBuffer = WriteBuffer.allocateSufficient(rectangle);
    pickler.serialize(rectangleBuffer, rectangle);
    assertFalse(rectangleBuffer.hasRemaining());
    buf = ReadBuffer.wrap(rectangleBuffer.flip());
    Shape deserializedRectangle = pickler.deserialize(buf);

    assertInstanceOf(Rectangle.class, deserializedRectangle);
    assertEquals(rectangle, deserializedRectangle);
    assertEquals(4.0, ((Rectangle) deserializedRectangle).width());
    assertEquals(6.0, ((Rectangle) deserializedRectangle).height());

    // Test triangle
    final var triangleBuffer = WriteBuffer.allocateSufficient(triangle);
    pickler.serialize(triangleBuffer, triangle);
    assertFalse(triangleBuffer.hasRemaining());
    buf = ReadBuffer.wrap(triangleBuffer.flip());
    Shape deserializedTriangle = pickler.deserialize(buf);

    assertInstanceOf(Triangle.class, deserializedTriangle);
    assertEquals(triangle, deserializedTriangle);
    assertEquals(3.0, ((Triangle) deserializedTriangle).a());
    assertEquals(4.0, ((Triangle) deserializedTriangle).b());
    assertEquals(5.0, ((Triangle) deserializedTriangle).c());
  }

  @Test
  void testNestedSealedInterfaces() {
    // Create instances of different Animal implementations
    Animal dog = new Dog("Buddy", 3);
    Animal cat = new Cat("Whiskers", true);
    Animal eagle = new Eagle(2.1);
    Animal penguin = new Penguin(true);
    Animal alicorn = new Alicorn("Twilight Sparkle", new String[]{"elements of harmony", "wings of a pegasus"});

    // Get a pickler for the top-level Animal sealed interface
    Pickler<Animal> pickler = Pickler.forSealedInterface(Animal.class);

    // Test dog serialization/deserialization
    final var dogBuffer = WriteBuffer.of(1024);
    pickler.serialize(dogBuffer, dog);
    int bytesWritten = dogBuffer.position();
    var buf = ReadBuffer.wrap(dogBuffer.flip());
    Animal deserializedDog = pickler.deserialize(buf);

    assertInstanceOf(Dog.class, deserializedDog);
    assertEquals(dog, deserializedDog);
    assertEquals("Buddy", ((Dog) deserializedDog).name());
    assertEquals(3, ((Dog) deserializedDog).age());

    // Test cat serialization/deserialization
    final var catBuffer = WriteBuffer.of(1024);
    pickler.serialize(catBuffer, cat);
    assertFalse(catBuffer.hasRemaining());
    buf = ReadBuffer.wrap(catBuffer.flip());
    Animal deserializedCat = pickler.deserialize(buf);

    assertInstanceOf(Cat.class, deserializedCat);
    assertEquals(cat, deserializedCat);
    assertEquals("Whiskers", ((Cat) deserializedCat).name());
    assertTrue(((Cat) deserializedCat).purrs());

    // Test eagle serialization/deserialization
    final var eagleBuffer = WriteBuffer.allocateSufficient(eagle);
    pickler.serialize(eagleBuffer, eagle);
    assertFalse(eagleBuffer.hasRemaining());
    buf = ReadBuffer.wrap(eagleBuffer.flip());
    Animal deserializedEagle = pickler.deserialize(buf);

    assertInstanceOf(Eagle.class, deserializedEagle);
    assertEquals(eagle, deserializedEagle);
    assertEquals(2.1, ((Eagle) deserializedEagle).wingspan());

    // Test penguin serialization/deserialization
    final var penguinBuffer = WriteBuffer.allocateSufficient(penguin);
    pickler.serialize(penguinBuffer, penguin);
    assertFalse(penguinBuffer.hasRemaining());
    buf = ReadBuffer.wrap(penguinBuffer.flip());
    Animal deserializedPenguin = pickler.deserialize(buf);

    assertInstanceOf(Penguin.class, deserializedPenguin);
    assertEquals(penguin, deserializedPenguin);
    assertTrue(((Penguin) deserializedPenguin).canSwim());

    // Test alicorn serialization/deserialization
    final var alicornBuffer = WriteBuffer.allocateSufficient(alicorn);
    pickler.serialize(alicornBuffer, alicorn);
    assertFalse(alicornBuffer.hasRemaining());
    buf = ReadBuffer.wrap(alicornBuffer.flip());

    Animal deserializedAlicorn = pickler.deserialize(buf);
    assertInstanceOf(Alicorn.class, deserializedAlicorn);
    assertArrayEquals(new String[]{"elements of harmony", "wings of a pegasus"}, ((Alicorn) deserializedAlicorn).magicPowers());
  }

  /// Tests the protocol example from the README
  @Test
  void testProtocolExample() {

    // Get picklers for the protocol interfaces
    Pickler<StackCommand> commandPickler = Pickler.forSealedInterface(StackCommand.class);

    // Test Command serialization/deserialization
    StackCommand[] commands = {
        new Push("test-item"),
        new Pop(),
        new Peek()
    };

    var buffer = WriteBuffer.allocateSufficient(1024);

    for (StackCommand command : commands) {
      commandPickler.serialize(buffer, command);

      var buf = ReadBuffer.wrap(buffer.flip());
      StackCommand deserializedCommand = commandPickler.deserialize(buf);
      assertEquals(command, deserializedCommand);
    }

    // Test Response serialization/deserialization
    StackResponse[] responses = {
        new Success(Optional.of("item-value")),
        new Success(Optional.empty()),
        new Failure("operation failed")
    };

    Pickler<StackResponse> responsePickler = Pickler.forSealedInterface(StackResponse.class);

    buffer = WriteBuffer.allocateSufficient(1024);
    for (StackResponse response : responses) {
      responsePickler.serialize(buffer, response);

      var buf = ReadBuffer.wrap(buffer.flip());
      StackResponse deserializedResponse = responsePickler.deserialize(buf);
      assertEquals(response, deserializedResponse);
      assertEquals(response.payload(), deserializedResponse.payload());
    }

    // Simulate a client-server interaction
    // Client sends a Push command
    StackCommand clientCommand = new Push("important-data");
    final var commandBuffer = WriteBuffer.of(1024);
    commandPickler.serialize(commandBuffer, clientCommand);
    var buf = ReadBuffer.wrap(commandBuffer.flip());

    // Server receives and processes the command
    StackCommand receivedCommand = commandPickler.deserialize(buf);
    assertInstanceOf(Push.class, receivedCommand);
    assertEquals("important-data", ((Push) receivedCommand).item());

    // Server sends back a success response
    StackResponse serverResponse = new Success(Optional.of("operation successful"));
    final var responseBuffer = WriteBuffer.of(1024);
    responsePickler.serialize(responseBuffer, serverResponse);
    buf = ReadBuffer.wrap(responseBuffer.flip());

    // Client receives and processes the response
    StackResponse receivedResponse = responsePickler.deserialize(buf);
    assertInstanceOf(Success.class, receivedResponse);
    assertEquals("operation successful", receivedResponse.payload());
  }

  static StringBuilder stripOutAsciiStrings(byte[] bytes) {
    StringBuilder escapedSearchString = new StringBuilder();

    for (byte b : bytes) {
      // Check if the byte is a printable ASCII character (32-126)
      if (b >= 32 && b <= 126) {
        escapedSearchString.append((char) b);
      }
    }
    return escapedSearchString;
  }

  @Test
  void testOptionalsContainingArrays() {
    // Create optionals containing arrays
    Optional<int[]> optionalIntArray = Optional.of(new int[]{1, 2, 3, 4, 5});
    Optional<String[]> optionalStringArray = Optional.of(new String[]{"Hello", "World"});
    Optional<Person[]> optionalPersonArray = Optional.of(new Person[]{
        new Person("Alice", 30),
        new Person("Bob", 25)
    });
    Optional<int[]> emptyOptionalArray = Optional.empty();

    // Create a record to hold these optionals
    record ArrayOptionalsRecord(
        Optional<int[]> optionalIntArray,
        Optional<String[]> optionalStringArray,
        Optional<Person[]> optionalPersonArray,
        Optional<int[]> emptyOptionalArray
    ) {
    }

    // Create an instance
    ArrayOptionalsRecord original = new ArrayOptionalsRecord(
        optionalIntArray, optionalStringArray, optionalPersonArray, emptyOptionalArray);

    // Get a pickler for the record
    Pickler<ArrayOptionalsRecord> pickler = Pickler.forRecord(ArrayOptionalsRecord.class);

    // Calculate size and allocate buffer

    final var buffer = WriteBuffer.allocateSufficient(original);

    // Serialize
    pickler.serialize(buffer, original);

    var buf = ReadBuffer.wrap(buffer.flip());

    // Deserialize
    ArrayOptionalsRecord deserialized = pickler.deserialize(buf);

    // Verify optionals presence
    assertEquals(original.optionalIntArray().isPresent(), deserialized.optionalIntArray().isPresent());
    assertEquals(original.optionalStringArray().isPresent(), deserialized.optionalStringArray().isPresent());
    assertEquals(original.optionalPersonArray().isPresent(), deserialized.optionalPersonArray().isPresent());
    assertEquals(original.emptyOptionalArray().isPresent(), deserialized.emptyOptionalArray().isPresent());

    // Verify int array content
    if (original.optionalIntArray().isPresent()) {
      assertArrayEquals(original.optionalIntArray().get(), deserialized.optionalIntArray().get());
    }

    // Verify string array content
    if (original.optionalStringArray().isPresent()) {
      assertArrayEquals(original.optionalStringArray().get(), deserialized.optionalStringArray().get());
    }

    // Verify person array content
    if (original.optionalPersonArray().isPresent()) {
      Person[] originalPersons = original.optionalPersonArray().get();
      Person[] deserializedPersons = deserialized.optionalPersonArray().get();
      assertEquals(originalPersons.length, deserializedPersons.length);
      IntStream.range(0, originalPersons.length)
          .forEach(i -> assertEquals(originalPersons[i], deserializedPersons[i]));
    }

    // Verify empty optional
    assertTrue(deserialized.emptyOptionalArray().isEmpty());
  }

  // Define simple enums for testing
  enum TestColor {
    RED, GREEN, BLUE, YELLOW
  }

  enum TestSize {
    SMALL, MEDIUM, LARGE, EXTRA_LARGE
  }

  @Test
  void testEnumArray() {
    // Create a record with an array of enums
    record EnumArrayRecord(TestColor[] colors) {
    }

    // Create an instance
    EnumArrayRecord original = new EnumArrayRecord(
        new TestColor[]{TestColor.RED, TestColor.GREEN, TestColor.BLUE, TestColor.YELLOW}
    );

    // Get a pickler for the record
    Pickler<EnumArrayRecord> pickler = Pickler.forRecord(EnumArrayRecord.class);

    // Calculate size and allocate buffer
    //
    final var buffer = WriteBuffer.of(1024);

    // Serialize
    pickler.serialize(buffer, original);
//    
    var buf = ReadBuffer.wrap(buffer.flip());

    // Deserialize
    EnumArrayRecord deserialized = pickler.deserialize(buf);

    // Verify array length
    assertEquals(original.colors().length, deserialized.colors().length);

    // Verify each enum value
    for (int i = 0; i < original.colors().length; i++) {
      assertEquals(original.colors()[i], deserialized.colors()[i]);
    }
  }

  @Test
  void testOptionalEnum() {
    // Create a record with optional enum fields
    record OptionalEnumRecord(
        Optional<TestColor> colorOpt,
        Optional<TestSize> sizeOpt,
        Optional<TestColor> emptyColorOpt
    ) {
    }

    // Create an instance
    OptionalEnumRecord original = new OptionalEnumRecord(
        Optional.of(TestColor.YELLOW),
        Optional.of(TestSize.MEDIUM),
        Optional.empty()
    );

    // Get a pickler for the record
    Pickler<OptionalEnumRecord> pickler = Pickler.forRecord(OptionalEnumRecord.class);

    // Calculate size and allocate buffer
    final var buffer = WriteBuffer.allocateSufficient(original);

    // Serialize
    pickler.serialize(buffer, original);

    var buf = ReadBuffer.wrap(buffer.flip());

    // Deserialize
    OptionalEnumRecord deserialized = pickler.deserialize(buf);

    // Verify optional enum values
    assertEquals(original.colorOpt(), deserialized.colorOpt());
    assertEquals(original.sizeOpt(), deserialized.sizeOpt());
    assertEquals(original.emptyColorOpt(), deserialized.emptyColorOpt());

    // Verify specific values
    assertTrue(deserialized.colorOpt().isPresent());
    assertEquals(TestColor.YELLOW, deserialized.colorOpt().get());
    assertTrue(deserialized.sizeOpt().isPresent());
    assertEquals(TestSize.MEDIUM, deserialized.sizeOpt().get());
    assertTrue(deserialized.emptyColorOpt().isEmpty());
  }

  @SuppressWarnings("OptionalGetWithoutIsPresent")
  @Test
  void testMixedEnumArray() {
    // Create a record with a mixed array containing enums
    record MixedRecord(Object[] mixedArray, Optional<Object[]> optionalMixedArray) {
    }

    // Create mixed arrays with enums, primitives, and strings
    Object[] mixedArray = new Object[]{
        TestColor.RED,
        42,
        "Hello",
        TestSize.EXTRA_LARGE,
        true
    };

    Object[] nestedArray = new Object[]{
        TestColor.GREEN,
        123,
        TestSize.SMALL
    };

    // Create an instance
    MixedRecord original = new MixedRecord(
        mixedArray,
        Optional.of(nestedArray)
    );

    // Get a pickler for the record
    Pickler<MixedRecord> pickler = Pickler.forRecord(MixedRecord.class);
    final var buffer = WriteBuffer.of(1024);
    LOGGER.info("Starting size calculation: ");
    LOGGER.info("Starting serialization");
    pickler.serialize(buffer, original);
    var buf = ReadBuffer.wrap(buffer.flip());

    // Deserialize
    MixedRecord deserialized = pickler.deserialize(buf);

    // Verify array lengths
    assertEquals(original.mixedArray().length, deserialized.mixedArray().length);
    assertTrue(deserialized.optionalMixedArray().isPresent());
    assertEquals(original.optionalMixedArray().get().length,
        deserialized.optionalMixedArray().get().length);

    // Verify each element in the main array
    for (int i = 0; i < original.mixedArray().length; i++) {
      assertEquals(original.mixedArray()[i], deserialized.mixedArray()[i]);
    }

    // Verify each element in the optional array
    for (int i = 0; i < original.optionalMixedArray().get().length; i++) {
      assertEquals(original.optionalMixedArray().get()[i],
          deserialized.optionalMixedArray().get()[i]);
    }

    // Verify specific enum values
    assertEquals(TestColor.RED, deserialized.mixedArray()[0]);
    assertEquals(TestSize.EXTRA_LARGE, deserialized.mixedArray()[3]);
    assertEquals(TestColor.GREEN, deserialized.optionalMixedArray().get()[0]);
    assertEquals(TestSize.SMALL, deserialized.optionalMixedArray().get()[2]);
  }
}
