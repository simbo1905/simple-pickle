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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/// Test class for the Pickler functionality.
/// Demonstrates basic serialization and deserialization of records.
class PicklerTest {

  /// Set up logging before all tests
  @BeforeAll
  static void setupLogging() {
    // Configure the root logger to use FINE level
    Logger rootLogger = Logger.getLogger("");
    rootLogger.setLevel(Level.FINE);

    // Make sure the console handler also uses FINE level
    for (java.util.logging.Handler handler : rootLogger.getHandlers()) {
      if (handler instanceof ConsoleHandler) {
        handler.setLevel(Level.FINE);
      }
    }

    // Configure the PicklerGenerator logger specifically
    Logger picklerLogger = Logger.getLogger("io.github.simbo1905.simple_pickle.PicklerGenerator");
    picklerLogger.setLevel(Level.FINE);

    // Set java.lang package logging to INFO or higher to hide shutdown messages
    Logger.getLogger("java.lang").setLevel(Level.INFO);
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
    Address address = new Address("123 Main St", "Anytown", "12345");
    Employee employee = new Employee("E12345", person, address);

    // Get a pickler for the Employee record
    Pickler<Employee> pickler = Pickler.picklerForRecord(Employee.class);

    // Serialize the record with nested records
    final var buffer = ByteBuffer.allocate(1024);
    pickler.serialize(employee, buffer);
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
    assertEquals("Anytown", deserialized.address().city());
    assertEquals("12345", deserialized.address().zipCode());
  }

  @Test
  void testMultiLevelNestedRecord() {
    // Create nested records with multiple levels
    Person person1 = new Person("John Manager", 45);
    Address address1 = new Address("555 Boss Ave", "Managertown", "54321");
    Employee manager = new Employee("M98765", person1, address1);

    Person person2 = new Person("Jane Employee", 30);
    Address address2 = new Address("123 Work St", "Employeeville", "12345");
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
}
