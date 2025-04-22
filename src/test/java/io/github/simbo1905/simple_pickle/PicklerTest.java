// SPDX-FileCopyrightText: 2025 Simon Massey
// SPDX-License-Identifier: Apache-2.0
package io.github.simbo1905.simple_pickle;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.logging.ConsoleHandler;
import java.util.logging.Level;
import java.util.logging.Logger;

import static io.github.simbo1905.simple_pickle.Pickler.PicklerBase.LOGGER;
import static org.junit.jupiter.api.Assertions.*;

/// Test class for the Pickler functionality.
/// Demonstrates basic serialization and deserialization of records.
class PicklerTest {

  @BeforeAll
  static void setupLogging() {
    final var logLevel = System.getProperty("java.util.logging.ConsoleHandler.level", "FINER");
    final Level level = Level.parse(logLevel);

    LOGGER.setLevel(level);
    ConsoleHandler consoleHandler = new ConsoleHandler();
    consoleHandler.setLevel(level);
    LOGGER.addHandler(consoleHandler);

    // Configure SessionKeyManager logger
    Logger logger = Logger.getLogger(Pickler.class.getName());
    logger.setLevel(level);
    ConsoleHandler skmHandler = new ConsoleHandler();
    skmHandler.setLevel(level);
    logger.addHandler(skmHandler);

    // Optionally disable parent handlers if needed
    LOGGER.setUseParentHandlers(false);
    logger.setUseParentHandlers(false);

    LOGGER.info("Logging initialized at level: " + level);
  }
  /// A simple record for testing purposes
  record Person(String name, int age) {
  }

  /// A record with Optional fields for testing
  record OptionalExample(Optional<Object> objectOpt, Optional<Integer> intOpt, Optional<String> stringOpt) {
  }

  /// An empty record with no components for testing
  record Empty() {
  }

  /// A record with nullable fields for testing
  record NullableFieldsExample(String stringField, Integer integerField, Double doubleField, Object objectField) {
  }

  /// A record with a nested record for testing
  record Address(String street, String city, String zipCode) {
  }

  /// A record containing another record
  record Employee(String id, Person person, Address address) {
  }

  /// A record with multiple levels of nesting (modified to not use arrays)
  record Department(String name, Employee manager, Employee employee) {
  }

  /// A record with arrays for testing
  record ArrayExample(
      int[] intArray,
      String[] stringArray,
      boolean[] booleanArray,
      Person[] personArray,
      Integer[] boxedIntArray,
      Object[] mixedArray
  ) {
  }

  /// A record with nested arrays
  record NestedArrayExample(
      int[][] nestedIntArray,
      Object[][] nestedObjectArray
  ) {
  }

  /// Define a sealed interface for testing
  sealed interface Shape permits Circle, Rectangle, Triangle {
  }

  /// Record implementations of the sealed interface
  record Circle(double radius) implements Shape {
  }

  record Rectangle(double width, double height) implements Shape {
  }

  record Triangle(double a, double b, double c) implements Shape {
  }


  /// Define the protocol classes
  sealed interface StackCommand permits Push, Pop, Peek {
  }

  /// This must be public if it is not in the picker package
  record Push(String item) implements StackCommand {
  }

  /// This must be public if it is not in the picker package
  record Pop() implements StackCommand {
  }

  /// This must be public if it is not in the picker package
  record Peek() implements StackCommand {
  }

  sealed interface StackResponse permits Success, Failure {
    String payload();
  }

  /// This must be public if it is not in the picker package
  record Success(Optional<String> value) implements StackResponse {
    public String payload() {
      return value.orElse(null);
    }
  }

  /// This must be public if it is not in the picker package
  record Failure(String errorMessage) implements StackResponse {
    public String payload() {
      return errorMessage;
    }
  }


  /// Define a hierarchy of nested sealed interfaces for testing
  sealed interface Animal permits Mammal, Bird, Alicorn {
  }

  sealed interface Mammal extends Animal permits Dog, Cat {
  }

  sealed interface Bird extends Animal permits Eagle, Penguin {
  }

  record Alicorn(String name, String[] magicPowers) implements Animal {
  }

  record Dog(String name, int age) implements Mammal {
  }

  record Cat(String name, boolean purrs) implements Mammal {
  }

  record Eagle(double wingspan) implements Bird {
  }

  record Penguin(boolean canSwim) implements Bird {
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

    Pickler<Person> generated = Pickler.picklerForRecord(Person.class);

    // Serialize the Person record to a byte buffer
    final var buffer = ByteBuffer.allocate(1024);
    generated.serialize(original, buffer);
    buffer.flip(); // Prepare buffer for reading

    // Deserialize from the byte buffer
    final var deserialized = generated.deserialize(buffer);

    // Verify the deserialized object matches the original
    assertEquals(original, deserialized);
    assertEquals("Alice", deserialized.name());
    assertEquals(30, deserialized.age());
  }

  record Simple(int value) {
  }

  /// Tests manual implementation of Simple record serialization
  @Test
  void testGenericSimpleRecordStatics() {
    // Use the manually created pickler
    final var original = new Simple(42);

    Pickler<Simple> generated = Pickler.picklerForRecord(Simple.class);

    // Serialize the Simple record to a byte buffer
    final var buffer = ByteBuffer.allocate(1024);
    generated.serialize(original, buffer);
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

    Pickler<OptionalExample> pickler = Pickler.picklerForRecord(OptionalExample.class);

    // Serialize the record
    final var buffer = ByteBuffer.allocate(1024);
    pickler.serialize(original, buffer);
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
    Pickler<Empty> pickler = Pickler.picklerForRecord(Empty.class);

    // Serialize the empty record
    final var buffer = ByteBuffer.allocate(1024);
    pickler.serialize(original, buffer);
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

    Pickler<NullableFieldsExample> pickler = Pickler.picklerForRecord(NullableFieldsExample.class);

    // Serialize the record
    final var buffer = ByteBuffer.allocate(1024);
    pickler.serialize(original, buffer);
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
    Pickler<Employee> pickler = Pickler.picklerForRecord(Employee.class);

    // Serialize the record with nested records
    final var buffer = ByteBuffer.allocate(1024);
    pickler.serialize(employee, buffer);
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
    Pickler<Department> pickler = Pickler.picklerForRecord(Department.class);

    // Serialize the record with nested records
    final var buffer = ByteBuffer.allocate(2048);
    pickler.serialize(department, buffer);
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

  @Test
  void testArrays() {
    // Create a record with different array types
    final var original = new ArrayExample(
        new int[]{1, 2, 3, 4, 5},
        new String[]{"apple", "banana", "cherry"},
        new boolean[]{true, false, true},
        new Person[]{new Person("Alice", 30), new Person("Bob", 25)},
        new Integer[]{10, 20, null, 40},
        new Object[]{42, "mixed", true, null}
    );

    Pickler<ArrayExample> pickler = Pickler.picklerForRecord(ArrayExample.class);

    // Serialize the record
    final var buffer = ByteBuffer.allocate(2048);
    pickler.serialize(original, buffer);
    buffer.flip(); // Prepare buffer for reading

    // Deserialize from the byte buffer
    final var deserialized = pickler.deserialize(buffer);

    // Replace direct equality check with component-by-component array comparison
    assertArrayRecordEquals(original, deserialized);

    // Individual array checks are already handled by assertArrayRecordEquals
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

    Pickler<ArrayExample> pickler = Pickler.picklerForRecord(ArrayExample.class);

    // Serialize the record
    final var buffer = ByteBuffer.allocate(1024);
    pickler.serialize(original, buffer);
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

    Pickler<NestedArrayExample> pickler = Pickler.picklerForRecord(NestedArrayExample.class);

    // Serialize the record
    final var buffer = ByteBuffer.allocate(2048);
    pickler.serialize(original, buffer);
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
    Pickler<Shape> pickler = Pickler.picklerForSealedTrait(Shape.class);

    // Test circle
    ByteBuffer circleBuffer = ByteBuffer.allocate(1024);
    pickler.serialize(circle, circleBuffer);
    circleBuffer.flip();
    Shape deserializedCircle = pickler.deserialize(circleBuffer);

    assertInstanceOf(Circle.class, deserializedCircle);
    assertEquals(circle, deserializedCircle);
    assertEquals(5.0, ((Circle) deserializedCircle).radius());

    // Test rectangle
    ByteBuffer rectangleBuffer = ByteBuffer.allocate(1024);
    pickler.serialize(rectangle, rectangleBuffer);
    rectangleBuffer.flip();
    Shape deserializedRectangle = pickler.deserialize(rectangleBuffer);

    assertInstanceOf(Rectangle.class, deserializedRectangle);
    assertEquals(rectangle, deserializedRectangle);
    assertEquals(4.0, ((Rectangle) deserializedRectangle).width());
    assertEquals(6.0, ((Rectangle) deserializedRectangle).height());

    // Test triangle
    ByteBuffer triangleBuffer = ByteBuffer.allocate(1024);
    pickler.serialize(triangle, triangleBuffer);
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
    Pickler<Shape> pickler = Pickler.picklerForSealedTrait(Shape.class);

    // Serialize null
    ByteBuffer buffer = ByteBuffer.allocate(1024);
    pickler.serialize(null, buffer);
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
    Pickler<Animal> pickler = Pickler.picklerForSealedTrait(Animal.class);

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
    ByteBuffer catBuffer = ByteBuffer.allocate(1024);
    pickler.serialize(cat, catBuffer);
    catBuffer.flip();
    Animal deserializedCat = pickler.deserialize(catBuffer);

    assertInstanceOf(Cat.class, deserializedCat);
    assertEquals(cat, deserializedCat);
    assertEquals("Whiskers", ((Cat) deserializedCat).name());
    assertTrue(((Cat) deserializedCat).purrs());

    // Test eagle serialization/deserialization
    ByteBuffer eagleBuffer = ByteBuffer.allocate(1024);
    pickler.serialize(eagle, eagleBuffer);
    eagleBuffer.flip();
    Animal deserializedEagle = pickler.deserialize(eagleBuffer);

    assertInstanceOf(Eagle.class, deserializedEagle);
    assertEquals(eagle, deserializedEagle);
    assertEquals(2.1, ((Eagle) deserializedEagle).wingspan());

    // Test penguin serialization/deserialization
    ByteBuffer penguinBuffer = ByteBuffer.allocate(1024);
    pickler.serialize(penguin, penguinBuffer);
    penguinBuffer.flip();
    Animal deserializedPenguin = pickler.deserialize(penguinBuffer);

    assertInstanceOf(Penguin.class, deserializedPenguin);
    assertEquals(penguin, deserializedPenguin);
    assertTrue(((Penguin) deserializedPenguin).canSwim());

    // Test alicorn serialization/deserialization
    ByteBuffer alicornBuffer = ByteBuffer.allocate(1024);
    pickler.serialize(alicorn, alicornBuffer);
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
    Pickler<StackCommand> commandPickler = Pickler.picklerForSealedTrait(StackCommand.class);
    Pickler<StackResponse> responsePickler = Pickler.picklerForSealedTrait(StackResponse.class);

    // Test Command serialization/deserialization
    StackCommand[] commands = {
        new Push("test-item"),
        new Pop(),
        new Peek()
    };

    for (StackCommand command : commands) {
      ByteBuffer buffer = ByteBuffer.allocate(1024);
      commandPickler.serialize(command, buffer);
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

    for (StackResponse response : responses) {
      ByteBuffer buffer = ByteBuffer.allocate(1024);
      responsePickler.serialize(response, buffer);
      int bytesWritten = buffer.position();
      assertEquals(bytesWritten, responsePickler.sizeOf(response));
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

}
