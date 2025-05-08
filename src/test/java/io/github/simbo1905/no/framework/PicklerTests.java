// SPDX-FileCopyrightText: 2025 Simon Massey
// SPDX-License-Identifier: Apache-2.0
package io.github.simbo1905.no.framework;

import io.github.simbo1905.no.framework.animal.*;
import io.github.simbo1905.no.framework.model.*;
import io.github.simbo1905.no.framework.protocol.*;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.logging.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.IntStream;

import static io.github.simbo1905.no.framework.Companion.resolveClass;
import static io.github.simbo1905.no.framework.Companion.writeDeduplicatedClassName;
import static io.github.simbo1905.no.framework.Constants.ARRAY;
import static io.github.simbo1905.no.framework.Pickler.LOGGER;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.*;

/// Test class for the Pickler functionality.
/// Demonstrates basic serialization and deserialization of records.
class PicklerTests {

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
   * Utility method to check array record equality by comparing each component
   * @param expected The expected array record
   * @param actual The actual array record
   */
  private void assertArrayRecordEquals(ArrayExample expected, ArrayExample actual) {
    assertArrayEquals(expected.intArray(), actual.intArray());
    assertArrayEquals(expected.stringArray(), actual.stringArray());
    assertArrayEquals(expected.booleanArray(), actual.booleanArray());
    assertDeepArrayEquals(expected.personArray(), actual.personArray());
    assertArrayEquals(expected.boxedIntArray(), actual.boxedIntArray());
    assertDeepArrayEquals(expected.mixedArray(), actual.mixedArray());
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
  private void assertDeepArrayEquals(Object[] expected, Object[] actual) {
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
  void testGenericPersonOneShot() {
    // Use the generic pickler
    final var original = new Person("Alice", 30);

    Pickler<Person> generated = Pickler.forRecord(Person.class);

    // Serialize the Person record to a byte buffer
    int size = generated.sizeOf(original);
    final var buffer = ByteBuffer.allocate(size);
    generated.serialize(original, buffer);
    assertFalse(buffer.hasRemaining());
    buffer.flip(); // Prepare buffer for reading

    // Deserialize from the byte buffer
    final var deserialized = generated.deserialize(buffer);

    // Verify the deserialized object matches the original
    assertEquals(original, deserialized);
    assertEquals("Alice", deserialized.name());
    assertEquals(30, deserialized.age());
  }


  /// Tests manual implementation of Simple record serialization
  @Test
  void testGenericSimpleRecordStatics() {
    // Use the manually created pickler
    final var original = new Simple(42);

    Pickler<Simple> generated = Pickler.forRecord(Simple.class);
    int size = generated.sizeOf(original);
    // Serialize the Simple record to a byte buffer
    final var buffer = ByteBuffer.allocate(size);
    generated.serialize(original, buffer);
    assertFalse(buffer.hasRemaining());
    buffer.flip(); // Prepare buffer for reading

    // Deserialize from the byte buffer
    final var deserialized = generated.deserialize(buffer);

    // Verify the deserialized object matches the original
    assertEquals(original, deserialized);
    assertEquals(42, deserialized.value());
  }

  @Test
  void testOptionalFields() {
    // Create a record with different Optional scenarios
    final var original = new OptionalExample(
        Optional.empty(),              // Empty optional
        Optional.of(42),               // Optional with Integer
        Optional.of("Hello, World!")   // Optional with String
    );

    Pickler<OptionalExample> pickler = Pickler.forRecord(OptionalExample.class);

    // Serialize the record
    final var buffer = ByteBuffer.allocate(pickler.sizeOf(original));
    pickler.serialize(original, buffer);
    assertFalse(buffer.hasRemaining());
    buffer.flip(); // Prepare buffer for reading

    // Deserialize from the byte buffer
    final var deserialized = pickler.deserialize(buffer);

    // Verify the deserialized object matches the original
    assertEquals(original, deserialized);
    assertEquals(Optional.empty(), deserialized.objectOpt());
    assertEquals(Optional.of(42), deserialized.intOpt());
    assertEquals(Optional.of("Hello, World!"), deserialized.stringOpt());
  }

  @Test
  void testEmptyRecord() {
    // Create an instance of the empty record
    final var original = new Empty();

    // Get a pickler for the empty record
    Pickler<Empty> pickler = Pickler.forRecord(Empty.class);

    // Serialize the empty record
    final var buffer = ByteBuffer.allocate(pickler.sizeOf(original));
    pickler.serialize(original, buffer);
    assertFalse(buffer.hasRemaining());
    buffer.flip(); // Prepare buffer for reading

    // Deserialize from the byte buffer
    final var deserialized = pickler.deserialize(buffer);

    // Verify the deserialized object matches the original
    assertEquals(original, deserialized);
  }

  @Test
  void testNullValues() {
    // Create a record with different null field combinations
    final var original = new NullableFieldsExample(null, 42, null, null);

    Pickler<NullableFieldsExample> pickler = Pickler.forRecord(NullableFieldsExample.class);

    // Serialize the record
    final var buffer = ByteBuffer.allocate(pickler.sizeOf(original));
    pickler.serialize(original, buffer);
    assertFalse(buffer.hasRemaining());
    buffer.flip(); // Prepare buffer for reading

    // Deserialize from the byte buffer
    final var deserialized = pickler.deserialize(buffer);

    // Verify the deserialized object matches the original
    assertEquals(original, deserialized);
    assertNull(deserialized.stringField());
    assertEquals(Integer.valueOf(42), deserialized.integerField());
    assertNull(deserialized.doubleField());
    assertNull(deserialized.objectField());
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
    final var buffer = ByteBuffer.allocate(pickler.sizeOf(employee));
    pickler.serialize(employee, buffer);
    assertFalse(buffer.hasRemaining());
    int bytesWritten = buffer.position();
    assertEquals(bytesWritten, pickler.sizeOf(employee));
    buffer.flip(); // Prepare buffer for reading

    // Deserialize from the byte buffer
    final var deserialized = pickler.deserialize(buffer);

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
    final var buffer = ByteBuffer.allocate(pickler.sizeOf(department));
    pickler.serialize(department, buffer);
    assertFalse(buffer.hasRemaining());
    buffer.flip(); // Prepare buffer for reading

    // Deserialize from the byte buffer
    final var deserialized = pickler.deserialize(buffer);

    // Verify the deserialized object matches the original
    assertEquals(department, deserialized);
    assertEquals("Engineering", deserialized.name());
    assertEquals(manager, deserialized.manager());
    assertEquals("John Manager", deserialized.manager().person().name());
    assertEquals(employee, deserialized.employee());
    assertEquals("Jane Employee", deserialized.employee().person().name());
  }

  public record SimpleArrayExample(int[] array1, int[] array2) {
  }

  @Test
  void testSimpleArray() {
    // Create a record with a simple array
    final var original = new SimpleArrayExample(new int[]{1, 2}, null);

    Pickler<SimpleArrayExample> pickler = Pickler.forRecord(SimpleArrayExample.class);

    int size = pickler.sizeOf(original);

    // Serialize the record
    var buffer = ByteBuffer.allocate(size);
    pickler.serialize(original, buffer);
    assertFalse(buffer.hasRemaining());
    buffer.flip(); // Prepare buffer for reading

    // Get the bytes from the buffer
    final var bytes = buffer.array();
    StringBuilder escapedSearchString = stripOutAsciiStrings(bytes);
    Matcher matcher = Pattern.compile(Constants.INTEGER._class().getName()).matcher(escapedSearchString.toString());
    int count = 0;
    while (matcher.find()) {
      count++;
    }
    assertEquals(1, count);
    buffer = ByteBuffer.wrap(bytes);

    // Deserialize from the byte buffer
    final var deserialized = pickler.deserialize(buffer);

    // Don't use assertEquals on the record directly as it will compare array references
    // Instead, compare the arrays individually using assertArrayEquals
    assertArrayEquals(original.array1(), deserialized.array1());
    assertArrayEquals(original.array2(), deserialized.array2());
  }


  @Test
  void testEmptyArrays() {
    // Create a record with empty arrays
    final var original = new ArrayExample(
        new int[]{},
        new String[]{},
        new boolean[]{},
        new Person[]{},
        new Integer[]{},
        new Object[]{}
    );

    Pickler<ArrayExample> pickler = Pickler.forRecord(ArrayExample.class);

    // Serialize the record
    final var buffer = ByteBuffer.allocate(pickler.sizeOf(original));
    pickler.serialize(original, buffer);
    assertFalse(buffer.hasRemaining());
    buffer.flip(); // Prepare buffer for reading

    // Deserialize from the byte buffer
    final var deserialized = pickler.deserialize(buffer);

    // Replace direct equality check with component-by-component array comparison
    assertArrayRecordEquals(original, deserialized);

    // Verify empty array handling (redundant with assertArrayRecordEquals but keeping for clarity)
    assertEquals(0, deserialized.intArray().length);
    assertEquals(0, deserialized.stringArray().length);
    assertEquals(0, deserialized.personArray().length);
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
    final var buffer = ByteBuffer.allocate(pickler.sizeOf(original));
    pickler.serialize(original, buffer);
    assertFalse(buffer.hasRemaining());
    buffer.flip(); // Prepare buffer for reading

    // Deserialize from the byte buffer
    final var deserialized = pickler.deserialize(buffer);

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

    int size = pickler.sizeOf(circle);

    // Test circle
    ByteBuffer circleBuffer = ByteBuffer.allocate(size);
    pickler.serialize(circle, circleBuffer);
    assertFalse(circleBuffer.hasRemaining());
    circleBuffer.flip();
    Shape deserializedCircle = pickler.deserialize(circleBuffer);

    assertInstanceOf(Circle.class, deserializedCircle);
    assertEquals(circle, deserializedCircle);
    assertEquals(5.0, ((Circle) deserializedCircle).radius());

    // Test rectangle
    ByteBuffer rectangleBuffer = ByteBuffer.allocate(pickler.sizeOf(rectangle));
    pickler.serialize(rectangle, rectangleBuffer);
    assertFalse(rectangleBuffer.hasRemaining());
    rectangleBuffer.flip();
    Shape deserializedRectangle = pickler.deserialize(rectangleBuffer);

    assertInstanceOf(Rectangle.class, deserializedRectangle);
    assertEquals(rectangle, deserializedRectangle);
    assertEquals(4.0, ((Rectangle) deserializedRectangle).width());
    assertEquals(6.0, ((Rectangle) deserializedRectangle).height());

    // Test triangle
    ByteBuffer triangleBuffer = ByteBuffer.allocate(pickler.sizeOf(triangle));
    pickler.serialize(triangle, triangleBuffer);
    assertFalse(triangleBuffer.hasRemaining());
    triangleBuffer.flip();
    Shape deserializedTriangle = pickler.deserialize(triangleBuffer);

    assertInstanceOf(Triangle.class, deserializedTriangle);
    assertEquals(triangle, deserializedTriangle);
    assertEquals(3.0, ((Triangle) deserializedTriangle).a());
    assertEquals(4.0, ((Triangle) deserializedTriangle).b());
    assertEquals(5.0, ((Triangle) deserializedTriangle).c());
  }

  @Test
  void testNullSealedInterface() {
    // Get a pickler for the Shape sealed interface
    Pickler<Shape> pickler = Pickler.forSealedInterface(Shape.class);

    // Serialize null
    ByteBuffer buffer = ByteBuffer.allocate(1024);
    pickler.serialize(null, buffer);
    assertEquals(1, buffer.position()); // 1 byte for null marker
    buffer.flip();

    // Deserialize null
    Shape deserialized = pickler.deserialize(buffer);
    assertNull(deserialized);
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
    ByteBuffer dogBuffer = ByteBuffer.allocate(1024);
    pickler.serialize(dog, dogBuffer);
    int bytesWritten = dogBuffer.position();
    assertEquals(bytesWritten, pickler.sizeOf(dog));
    dogBuffer.flip();
    Animal deserializedDog = pickler.deserialize(dogBuffer);

    assertInstanceOf(Dog.class, deserializedDog);
    assertEquals(dog, deserializedDog);
    assertEquals("Buddy", ((Dog) deserializedDog).name());
    assertEquals(3, ((Dog) deserializedDog).age());

    // Test cat serialization/deserialization
    ByteBuffer catBuffer = ByteBuffer.allocate(pickler.sizeOf(cat));
    pickler.serialize(cat, catBuffer);
    assertFalse(catBuffer.hasRemaining());
    catBuffer.flip();
    Animal deserializedCat = pickler.deserialize(catBuffer);

    assertInstanceOf(Cat.class, deserializedCat);
    assertEquals(cat, deserializedCat);
    assertEquals("Whiskers", ((Cat) deserializedCat).name());
    assertTrue(((Cat) deserializedCat).purrs());

    // Test eagle serialization/deserialization
    ByteBuffer eagleBuffer = ByteBuffer.allocate(pickler.sizeOf(eagle));
    pickler.serialize(eagle, eagleBuffer);
    assertFalse(eagleBuffer.hasRemaining());
    eagleBuffer.flip();
    Animal deserializedEagle = pickler.deserialize(eagleBuffer);

    assertInstanceOf(Eagle.class, deserializedEagle);
    assertEquals(eagle, deserializedEagle);
    assertEquals(2.1, ((Eagle) deserializedEagle).wingspan());

    // Test penguin serialization/deserialization
    ByteBuffer penguinBuffer = ByteBuffer.allocate(pickler.sizeOf(penguin));
    pickler.serialize(penguin, penguinBuffer);
    assertFalse(penguinBuffer.hasRemaining());
    penguinBuffer.flip();
    Animal deserializedPenguin = pickler.deserialize(penguinBuffer);

    assertInstanceOf(Penguin.class, deserializedPenguin);
    assertEquals(penguin, deserializedPenguin);
    assertTrue(((Penguin) deserializedPenguin).canSwim());

    // Test alicorn serialization/deserialization
    ByteBuffer alicornBuffer = ByteBuffer.allocate(pickler.sizeOf(alicorn));
    pickler.serialize(alicorn, alicornBuffer);
    assertFalse(alicornBuffer.hasRemaining());
    int alicornBytesWritten = alicornBuffer.position();
    assertEquals(alicornBytesWritten, pickler.sizeOf(alicorn));
    alicornBuffer.flip();

    Animal deserializedAlicorn = pickler.deserialize(alicornBuffer);
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

    for (StackCommand command : commands) {
      int size = commandPickler.sizeOf(command);
      ByteBuffer buffer = ByteBuffer.allocate(size);
      commandPickler.serialize(command, buffer);
      assertFalse(buffer.hasRemaining());
      buffer.flip();
      StackCommand deserializedCommand = commandPickler.deserialize(buffer);
      assertEquals(command, deserializedCommand);
    }

    // Test Response serialization/deserialization
    StackResponse[] responses = {
        new Success(Optional.of("item-value")),
        new Success(Optional.empty()),
        new Failure("operation failed")
    };

    Pickler<StackResponse> responsePickler = Pickler.forSealedInterface(StackResponse.class);

    for (StackResponse response : responses) {
      int size = responsePickler.sizeOf(response);
      ByteBuffer buffer = ByteBuffer.allocate(size);
      responsePickler.serialize(response, buffer);
      assertFalse(buffer.hasRemaining());
      buffer.flip();
      StackResponse deserializedResponse = responsePickler.deserialize(buffer);
      assertEquals(response, deserializedResponse);
      assertEquals(response.payload(), deserializedResponse.payload());
    }

    // Simulate a client-server interaction
    // Client sends a Push command
    StackCommand clientCommand = new Push("important-data");
    ByteBuffer commandBuffer = ByteBuffer.allocate(1024);
    commandPickler.serialize(clientCommand, commandBuffer);
    commandBuffer.flip();

    // Server receives and processes the command
    StackCommand receivedCommand = commandPickler.deserialize(commandBuffer);
    assertInstanceOf(Push.class, receivedCommand);
    assertEquals("important-data", ((Push) receivedCommand).item());

    // Server sends back a success response
    StackResponse serverResponse = new Success(Optional.of("operation successful"));
    ByteBuffer responseBuffer = ByteBuffer.allocate(1024);
    responsePickler.serialize(serverResponse, responseBuffer);
    responseBuffer.flip();

    // Client receives and processes the response
    StackResponse receivedResponse = responsePickler.deserialize(responseBuffer);
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
  void testEmptyRecordArray() {
    // Create an empty array of records
    Person[] emptyArray = new Person[0];

    // Calculate size and allocate buffer
    int size = Pickler.sizeOfMany(emptyArray);
    ByteBuffer buffer = ByteBuffer.allocate(size);

    // Serialize the array
    Pickler.serializeMany(emptyArray, buffer);
    buffer.flip();

    // Deserialize the array
    @SuppressWarnings("MismatchedReadAndWriteOfArray") Person[] deserialized = Pickler.deserializeMany(Person.class, buffer).toArray(Person[]::new);

    // Verify the array is empty
    assertEquals(0, deserialized.length);

    // Verify buffer is fully consumed
    assertEquals(buffer.limit(), buffer.position());
  }

  @Test
  void testRecordArray() {

    List<Person> personList = List.of(
        new Person("Alice", 30),
        new Person("Bob", 25),
        new Person("Charlie", 40)
    );

    // Create an array of records
    Person[] people = personList.toArray(Person[]::new);

    // Calculate size and allocate buffer
    int size = Pickler.sizeOfMany(people);
    ByteBuffer buffer = ByteBuffer.allocate(size);

    // Serialize the array
    Pickler.serializeMany(people, buffer);
    buffer.flip();

    // Deserialize the array
    Person[] deserialized = Pickler.deserializeMany(Person.class, buffer).toArray(Person[]::new);

    // Verify array length
    assertEquals(people.length, deserialized.length);

    // Verify each element
    for (int i = 0; i < people.length; i++) {
      assertEquals(people[i], deserialized[i]);
    }

    // Verify buffer is fully consumed
    assertEquals(buffer.limit(), buffer.position());
  }

  @Test
  void testMixedRecordArray() {
    // Create an array of different shapes using the sealed interface
    Shape[] shapes = new Shape[]{
        new Circle(5.0),
        new Rectangle(4.0, 6.0),
        new Triangle(3.0, 4.0, 5.0)
    };

    // Calculate size for the array
    int size = 0;
    Pickler<Shape> pickler = Pickler.forSealedInterface(Shape.class);

    // 1 byte for ARRAY marker + 4 bytes for component type name length + component type name bytes
    size += 1 + 4 + Shape.class.getName().getBytes(UTF_8).length;

    // 4 bytes for array length
    size += 4;

    // Size of each element
    for (Shape shape : shapes) {
      size += pickler.sizeOf(shape);
    }

    ByteBuffer buffer = ByteBuffer.allocate(size);

    // Write array marker
    buffer.put(ARRAY.marker());

    // Write component type
    Map<Class<?>, Integer> class2BufferOffset = new HashMap<>();
    writeDeduplicatedClassName(Work.of(buffer), Shape.class, class2BufferOffset, Shape.class.getName());

    // Write array length
    buffer.putInt(shapes.length);

    // Write each element
    for (Shape shape : shapes) {
      pickler.serialize(shape, buffer);
    }

    buffer.flip();

    // Skip the array marker
    buffer.get();

    // Read component type
    Map<Integer, Class<?>> bufferOffset2Class = new HashMap<>();
    try {
      Class<?> componentType = resolveClass(Work.of(buffer), bufferOffset2Class);
      assertEquals(Shape.class, componentType);
    } catch (ClassNotFoundException e) {
      fail("Failed to read component type: " + e.getMessage());
    }

    // Read array length
    int length = buffer.getInt();
    assertEquals(shapes.length, length);

    // Read each element using IntStream instead of traditional for loop
    java.util.stream.IntStream.range(0, length)
        .forEach(i -> {
          Shape deserialized = pickler.deserialize(buffer);
          assertEquals(shapes[i], deserialized);
        });

    // Verify buffer is fully consumed
    assertEquals(buffer.limit(), buffer.position());
  }

  // https://www.perplexity.ai/search/b11ebab9-122c-4841-b4bd-1d55de721ebd
  @SafeVarargs
  public static <T> Optional<T>[] createOptionalArray(Optional<T>... elements) {
    return elements;
  }

  @Test
  void testOptionalOfOptional() {
    record OptionalOptionalInt(Optional<Optional<Integer>> value) {
    }

    final var original = new OptionalOptionalInt(Optional.of(Optional.of(99)));

    // Get a pickler for the record
    Pickler<OptionalOptionalInt> pickler = Pickler.forRecord(OptionalOptionalInt.class);

    // Calculate size and allocate buffer
    int size = pickler.sizeOf(original);
    ByteBuffer buffer = ByteBuffer.allocate(size);

    // Serialize
    pickler.serialize(original, buffer);
    assertFalse(buffer.hasRemaining());
    buffer.flip();

    // Deserialize
    OptionalOptionalInt deserialized = pickler.deserialize(buffer);

    //noinspection OptionalGetWithoutIsPresent
    assertEquals(original.value().get().get(), deserialized.value().get().get());
  }

  @Test
  void testArraysOfOptionals() {
    // Create arrays of Optional values with mixed present/empty values
    Optional<String>[] stringOptionals = createOptionalArray(
        Optional.of("Hello"),
        Optional.empty(),
        Optional.of("World")
    );

    Optional<Integer>[] intOptionals = createOptionalArray(
        Optional.of(42),
        Optional.empty(),
        Optional.of(123),
        Optional.of(456)
    );

    // Create a record to hold these arrays
    record OptionalArraysRecord(Optional<String>[] stringOptionals, Optional<Integer>[] intOptionals) {
    }

    // Create an instance
    OptionalArraysRecord original = new OptionalArraysRecord(stringOptionals, intOptionals);

    // Get a pickler for the record
    Pickler<OptionalArraysRecord> pickler = Pickler.forRecord(OptionalArraysRecord.class);

    // Calculate size and allocate buffer
    int size = pickler.sizeOf(original);
    ByteBuffer buffer = ByteBuffer.allocate(size);

    // Serialize
    pickler.serialize(original, buffer);
    assertFalse(buffer.hasRemaining());
    buffer.flip();

    // Deserialize
    OptionalArraysRecord deserialized = pickler.deserialize(buffer);

    // Verify arrays length
    assertEquals(original.stringOptionals().length, deserialized.stringOptionals().length);
    assertEquals(original.intOptionals().length, deserialized.intOptionals().length);

    // Verify string optionals content
    IntStream.range(0, original.stringOptionals().length)
        .forEach(i -> assertEquals(original.stringOptionals()[i], deserialized.stringOptionals()[i]));

    // Verify integer optionals content
    IntStream.range(0, original.intOptionals().length)
        .forEach(i -> assertEquals(original.intOptionals()[i], deserialized.intOptionals()[i]));

    // Verify buffer is fully consumed
    assertEquals(buffer.limit(), buffer.position());
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
    int size = pickler.sizeOf(original);
    ByteBuffer buffer = ByteBuffer.allocate(size);

    // Serialize
    pickler.serialize(original, buffer);
    assertFalse(buffer.hasRemaining());
    buffer.flip();

    // Deserialize
    ArrayOptionalsRecord deserialized = pickler.deserialize(buffer);

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

    // Verify buffer is fully consumed
    assertEquals(buffer.limit(), buffer.position());
  }

  @Test
  void testPrimitiveArrays() {
    // Create arrays of all primitive types
    byte[] byteArray = {1, 2, 3, 127, -128};
    short[] shortArray = {1, 2, 3, 32767, -32768};
    char[] charArray = {'a', 'b', 'c', '1', '2'};
    long[] longArray = {1L, 2L, 3L, Long.MAX_VALUE, Long.MIN_VALUE};
    float[] floatArray = {1.0f, 2.5f, 3.14f, Float.MAX_VALUE, Float.MIN_VALUE};
    double[] doubleArray = {1.0, 2.5, 3.14, Double.MAX_VALUE, Double.MIN_VALUE};

    // Create a record to hold these arrays
    record PrimitiveArraysRecord(
        byte[] byteArray,
        short[] shortArray,
        char[] charArray,
        long[] longArray,
        float[] floatArray,
        double[] doubleArray
    ) {
    }

    // Create an instance
    PrimitiveArraysRecord original = new PrimitiveArraysRecord(
        byteArray, shortArray, charArray, longArray, floatArray, doubleArray);

    // Get a pickler for the record
    Pickler<PrimitiveArraysRecord> pickler = Pickler.forRecord(PrimitiveArraysRecord.class);

    // Calculate size and allocate buffer
    int size = pickler.sizeOf(original);
    ByteBuffer buffer = ByteBuffer.allocate(size);

    // Serialize
    pickler.serialize(original, buffer);
    assertFalse(buffer.hasRemaining());
    buffer.flip();

    // Deserialize
    PrimitiveArraysRecord deserialized = pickler.deserialize(buffer);

    // Verify all arrays
    assertArrayEquals(original.byteArray(), deserialized.byteArray());
    assertArrayEquals(original.shortArray(), deserialized.shortArray());
    assertArrayEquals(original.charArray(), deserialized.charArray());
    assertArrayEquals(original.longArray(), deserialized.longArray());
    assertArrayEquals(original.floatArray(), deserialized.floatArray(), 0.0f);
    assertArrayEquals(original.doubleArray(), deserialized.doubleArray(), 0.0);

    // Verify buffer is fully consumed
    assertEquals(buffer.limit(), buffer.position());
  }

  @Test
  void testByteArray() {
    // Create arrays of all primitive types
    byte[] byteArray = {1, 2, 3, 127, -128};

    // Create a record to hold these arrays
    record PrimitiveArraysRecord(
        byte[] byteArray
    ) {
    }

    // Create an instance
    PrimitiveArraysRecord original = new PrimitiveArraysRecord(
        byteArray);

    // Get a pickler for the record
    Pickler<PrimitiveArraysRecord> pickler = Pickler.forRecord(PrimitiveArraysRecord.class);

    // Calculate size and allocate buffer
    int size = pickler.sizeOf(original);
    ByteBuffer buffer = ByteBuffer.allocate(size);

    // Serialize
    pickler.serialize(original, buffer);
    assertFalse(buffer.hasRemaining());
    buffer.flip();

    // Deserialize
    PrimitiveArraysRecord deserialized = pickler.deserialize(buffer);

    // Verify all arrays
    assertArrayEquals(original.byteArray(), deserialized.byteArray());

    // Verify buffer is fully consumed
    assertEquals(buffer.limit(), buffer.position());
  }


  // Define simple enums for testing
  enum TestColor {
    RED, GREEN, BLUE, YELLOW
  }

  enum TestSize {
    SMALL, MEDIUM, LARGE, EXTRA_LARGE
  }

  @Test
  void testBasicEnum() {
    // Create a record with enum fields
    record EnumRecord(TestColor color, TestSize size) {
    }

    // Create an instance
    EnumRecord original = new EnumRecord(TestColor.BLUE, TestSize.LARGE);

    // Get a pickler for the record
    Pickler<EnumRecord> pickler = Pickler.forRecord(EnumRecord.class);

    // Calculate size and allocate buffer
    int size = pickler.sizeOf(original);
    ByteBuffer buffer = ByteBuffer.allocate(size);

    // Serialize
    pickler.serialize(original, buffer);
    assertFalse(buffer.hasRemaining());
    buffer.flip();

    // Deserialize
    EnumRecord deserialized = pickler.deserialize(buffer);

    // Verify enum values
    assertEquals(original.color(), deserialized.color());
    assertEquals(original.size(), deserialized.size());

    // Verify buffer is fully consumed
    assertEquals(buffer.limit(), buffer.position());
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
    //int size = pickler.sizeOf(original);
    ByteBuffer buffer = ByteBuffer.allocate(1024);

    // Serialize
    pickler.serialize(original, buffer);
//    assertFalse(buffer.hasRemaining());
    buffer.flip();

    // Deserialize
    EnumArrayRecord deserialized = pickler.deserialize(buffer);

    // Verify array length
    assertEquals(original.colors().length, deserialized.colors().length);

    // Verify each enum value
    for (int i = 0; i < original.colors().length; i++) {
      assertEquals(original.colors()[i], deserialized.colors()[i]);
    }

    // Verify buffer is fully consumed
    assertEquals(buffer.limit(), buffer.position());
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
    int size = pickler.sizeOf(original);
    ByteBuffer buffer = ByteBuffer.allocate(size);

    // Serialize
    pickler.serialize(original, buffer);
    assertFalse(buffer.hasRemaining());
    buffer.flip();

    // Deserialize
    OptionalEnumRecord deserialized = pickler.deserialize(buffer);

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

    // Verify buffer is fully consumed
    assertEquals(buffer.limit(), buffer.position());
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
    ByteBuffer buffer = ByteBuffer.allocate(1024);
    LOGGER.info("Starting size calculation: ");
    int size2 = pickler.sizeOf(original);
    LOGGER.info("Starting serialization");
    pickler.serialize(original, buffer);
    int actualSize = buffer.position();
    buffer.flip();

    LOGGER.info("Serialization size: " + actualSize);
    assertEquals(actualSize, size2);

    // Deserialize
    MixedRecord deserialized = pickler.deserialize(buffer);

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

    // Verify buffer is fully consumed
    assertEquals(buffer.limit(), buffer.position());
  }

  // Define a complex enum that should be rejected
  enum ComplexEnum {
    @SuppressWarnings("unused") ONE(1), TWO(2), @SuppressWarnings("unused") THREE(3);

    public final int value;

    ComplexEnum(@SuppressWarnings("unused") int value) {
      this.value = value;
    }
  }

  @Test
  void testComplexEnum() {
    // Create a record with a complex enum field
    record ComplexEnumRecord(ComplexEnum value) {
    }

    // Create an instance
    ComplexEnumRecord original = new ComplexEnumRecord(ComplexEnum.TWO);

    Pickler<ComplexEnumRecord> pickler = Pickler.forRecord(ComplexEnumRecord.class);

    // This should never execute, but if it does, try to serializeMany
    int size = pickler.sizeOf(original);
    ByteBuffer buffer = ByteBuffer.allocate(size);
    pickler.serialize(original, buffer);
    assertFalse(buffer.hasRemaining());
    buffer.flip();
    ComplexEnumRecord deserialized = pickler.deserialize(buffer);
    assertEquals(original.value(), deserialized.value());
  }

  @Test
  void testVeryLargeRecord() {
    // Create a record with a large number of fields
    record LargeRecord(
        int field1, int field2, int field3, int field4, int field5,
        int field6, int field7, int field8, int field9, int field10,
        int field11, int field12, int field13, int field14, int field15,
        int field16, int field17, int field18, int field19, int field20,
        int field21, int field22, int field23, int field24, int field25,
        int field26, int field27, int field28, int field29, int field30,
        int field31, int field32, int field33, int field34, int field35,
        int field36, int field37, int field38, int field39, int field40,
        int field41, int field42, int field43, int field44, int field45,
        int field46, int field47, int field48, int field49, int field50,
        int field51, int field52, int field53, int field54, int field55,
        int field56, int field57, int field58, int field59, int field60,
        int field61, int field62, int field63, int field64, int field65,
        int field66, int field67, int field68, int field69, int field70,
        int field71, int field72, int field73, int field74, int field75,
        int field76, int field77, int field78, int field79, int field80,
        int field81, int field82, int field83, int field84, int field85,
        int field86, int field87, int field88, int field89, int field90,
        int field91, int field92, int field93, int field94, int field95,
        int field96, int field97, int field98, int field99, int field100,
        int field101, int field102, int field103, int field104, int field105,
        int field106, int field107, int field108, int field109, int field110,
        int field111, int field112, int field113, int field114, int field115,
        int field116, int field117, int field118, int field119, int field120,
        int field121, int field122, int field123, int field124, int field125,
        int field126, int field127, int field128, int field129
    ) {
    }
// Create an instance with dummy values
    LargeRecord original = new LargeRecord(
        1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
        21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40,
        41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60,
        61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 80,
        81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93, 94, 95, 96, 97, 98, 99, 100,
        101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 113, 114, 115, 116, 117, 118, 119, 120,
        121, 122, 123, 124, 125, 126, 127, 128, 129
    );
    final var pickler = Pickler.forRecord(LargeRecord.class);
    // Calculate size and allocate buffer
    int size = pickler.sizeOf(original);
    ByteBuffer buffer = ByteBuffer.allocate(size);
    // Serialize
    pickler.serialize(original, buffer);
    assertFalse(buffer.hasRemaining());
    buffer.flip();
    // Deserialize
    LargeRecord deserialized = pickler.deserialize(buffer);
    // Verify all fields
    assertEquals(original, deserialized);
  }
}
