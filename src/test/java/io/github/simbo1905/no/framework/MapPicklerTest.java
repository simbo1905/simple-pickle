// SPDX-FileCopyrightText: 2025 Simon Massey
// SPDX-License-Identifier: Apache-2.0
package io.github.simbo1905.no.framework;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.ConsoleHandler;
import java.util.logging.Level;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.*;

/// Tests for Map serialization and deserialization.
class MapPicklerTest {

  private static final Logger LOGGER = Logger.getLogger(MapPicklerTest.class.getName());

  static {
    // Set up logging
    LOGGER.setLevel(Level.FINE);
    ConsoleHandler handler = new ConsoleHandler();
    handler.setLevel(Level.FINE);
    LOGGER.addHandler(handler);
  }

  /// Simple Person record for testing.
  record Person(String name, int age) {
  }

  /// FamilyRelation record for testing nested record serialization.
  record FamilyRelation(String relationship, Person member) {
  }

  /// Test record containing a Map field.
  record MapContainer(Map<String, Integer> stringToInt) {
  }

  /// Test record containing a nested Map field.
  record NestedMapContainer(Map<String, Map<Integer, String>> nestedMap) {
  }

  /// Test record containing a Map with record values.
  record RecordMapContainer(Map<String, Person> personMap) {
  }

  /// Test record containing a Map with Person keys and nested Map values.
  record NestedFamilyMapContainer(Map<Person, Map<String, FamilyRelation>> familyMap) {
  }

  /// Record for documentation example
  record DocumentationContainer(Person subject, Map<String, Person> relationships) {
  }

  /// Helper method to serializeMany and deserialize a record
  /// @param original The original record to serializeMany
  /// @param <T> The record type
  /// @return The deserialized record
  private <T extends Record> T serializeAndDeserialize(T original) {
    // Get a pickler for the record type
    @SuppressWarnings("unchecked") final var pickler = Pickler.forRecord((Class<T>) original.getClass());

    // Calculate size and allocate buffer
    final var size = pickler.sizeOf(original);
    final var buffer = ByteBuffer.allocate(size);

    // Serialize
    pickler.serialize(original, buffer);
    buffer.flip();

    // Deserialize
    final var deserialized = pickler.deserialize(buffer);

    // Verify buffer is fully consumed
    assertEquals(buffer.limit(), buffer.position(), "Buffer not fully consumed");

    return deserialized;
  }

  /// Test basic Map serialization and deserialization.
  @Test
  void testBasicMap() {
    // Create a map with simple key-value pairs
    final var map = new HashMap<String, Integer>();
    map.put("one", 1);
    map.put("two", 2);
    map.put("three", 3);

    // Create a record containing the map
    final var original = new MapContainer(map);

    // Serialize and deserialize
    final var deserialized = serializeAndDeserialize(original);

    // Verify map content
    assertEquals(original.stringToInt().size(), deserialized.stringToInt().size(), "Map size mismatch");
    assertEquals(original.stringToInt().get("one"), deserialized.stringToInt().get("one"), "Value for 'one' mismatch");
    assertEquals(original.stringToInt().get("two"), deserialized.stringToInt().get("two"), "Value for 'two' mismatch");
    assertEquals(original.stringToInt().get("three"), deserialized.stringToInt().get("three"), "Value for 'three' mismatch");
    // Attempting to modify the deserialized map should throw UnsupportedOperationException
    assertThrows(UnsupportedOperationException.class, () -> {
      deserialized.stringToInt().put("four", 4);
    });
  }

  /// Test empty Map serialization and deserialization.
  @Test
  void testEmptyMap() {
    // Create a record containing an empty map
    final var original = new MapContainer(new HashMap<>());

    // Serialize and deserialize
    final var deserialized = serializeAndDeserialize(original);

    // Verify map is empty
    assertTrue(deserialized.stringToInt().isEmpty(), "Map should be empty");
  }

  /// Test null Map serialization and deserialization.
  @Test
  void testNullMap() {
    // Create a record with a null map
    final var original = new MapContainer(null);

    // Serialize and deserialize
    final var deserialized = serializeAndDeserialize(original);

    // Verify map is null
    assertNull(deserialized.stringToInt(), "Map should be null");
  }

  /// Helper method to create a nested map for testing
  private Map<String, Map<Integer, String>> createNestedTestMap() {
    final var nestedMap = new HashMap<String, Map<Integer, String>>();

    final var innerMap1 = new HashMap<Integer, String>();
    innerMap1.put(1, "one");
    innerMap1.put(2, "two");

    final var innerMap2 = new HashMap<Integer, String>();
    innerMap2.put(3, "three");
    innerMap2.put(4, "four");

    nestedMap.put("first", innerMap1);
    nestedMap.put("second", innerMap2);

    return nestedMap;
  }

  /// Test nested Map serialization and deserialization.
  @Test
  void testNestedMap() {
    // Create a record containing a nested map
    final var original = new NestedMapContainer(createNestedTestMap());

    // Serialize and deserialize
    final var deserialized = serializeAndDeserialize(original);

    // Verify nested map structure
    assertEquals(original.nestedMap().size(), deserialized.nestedMap().size(), "Outer map size mismatch");

    // Verify first inner map
    final var deserializedInnerMap1 = deserialized.nestedMap().get("first");
    final var originalInnerMap1 = original.nestedMap().get("first");
    assertEquals(originalInnerMap1.size(), deserializedInnerMap1.size(), "Inner map 1 size mismatch");
    assertEquals(originalInnerMap1.get(1), deserializedInnerMap1.get(1), "Value for key 1 in inner map 1 mismatch");
    assertEquals(originalInnerMap1.get(2), deserializedInnerMap1.get(2), "Value for key 2 in inner map 1 mismatch");

    // Verify second inner map
    final var deserializedInnerMap2 = deserialized.nestedMap().get("second");
    final var originalInnerMap2 = original.nestedMap().get("second");
    assertEquals(originalInnerMap2.size(), deserializedInnerMap2.size(), "Inner map 2 size mismatch");
    assertEquals(originalInnerMap2.get(3), deserializedInnerMap2.get(3), "Value for key 3 in inner map 2 mismatch");
    assertEquals(originalInnerMap2.get(4), deserializedInnerMap2.get(4), "Value for key 4 in inner map 2 mismatch");
  }

  /// Helper method to create a map with Person records
  private Map<String, Person> createPersonMap() {
    final var personMap = new HashMap<String, Person>();
    personMap.put("alice", new Person("Alice", 30));
    personMap.put("bob", new Person("Bob", 25));
    return personMap;
  }

  /// Test Map with record values serialization and deserialization.
  @Test
  void testMapWithRecordValues() {
    // Create a record containing a map with Person values
    final var original = new RecordMapContainer(createPersonMap());

    // Serialize and deserialize
    final var deserialized = serializeAndDeserialize(original);

    // Verify map content
    assertEquals(original.personMap().size(), deserialized.personMap().size(), "Map size mismatch");
    assertEquals(original.personMap().get("alice"), deserialized.personMap().get("alice"), "Person 'alice' mismatch");
    assertEquals(original.personMap().get("bob"), deserialized.personMap().get("bob"), "Person 'bob' mismatch");
  }

  /// Test for documentation example with nested family relationships
  @Test
  void testDocumentation() {
    // Create family members
    final var john = new Person("John", 40);
    final var michael = new Person("Michael", 65);
    final var sarah = new Person("Sarah", 63);

    // Create relationships map
    final var familyMap = new HashMap<String, Person>();
    familyMap.put("father", michael);
    familyMap.put("mother", sarah);

    // Create container with subject and relationships
    final var original = new DocumentationContainer(john, familyMap);

    // Serialize and deserialize
    final var deserialized = serializeAndDeserialize(original);

    // Verify subject and relationships
    assertEquals(original.subject(), deserialized.subject(), "Subject mismatch");
    assertEquals(original.relationships().size(), deserialized.relationships().size(), "Relationships map size mismatch");
    assertEquals(original.relationships().get("father"), deserialized.relationships().get("father"), "Father relationship mismatch");
    assertEquals(original.relationships().get("mother"), deserialized.relationships().get("mother"), "Mother relationship mismatch");
  }

  /// Helper method to create test family data
  private Map<Person, Map<String, FamilyRelation>> createFamilyTestData() {
    // Create Person objects
    final var john = new Person("John", 40);
    final var michael = new Person("Michael", 65);
    final var sarah = new Person("Sarah", 63);
    final var emma = new Person("Emma", 38);

    // Create FamilyRelation objects
    final var father = new FamilyRelation("father", michael);
    final var mother = new FamilyRelation("mother", sarah);
    final var sister = new FamilyRelation("sister", emma);

    // Create nested maps
    final var family1 = new HashMap<String, FamilyRelation>();
    family1.put("father", father);
    family1.put("mother", mother);

    final var family2 = new HashMap<String, FamilyRelation>();
    family2.put("sister", sister);

    // Create the outer map with Person keys
    final var familyMap = new HashMap<Person, Map<String, FamilyRelation>>();
    familyMap.put(john, family1);
    familyMap.put(emma, family2);

    return familyMap;
  }

  /// Helper method to find a family map for a specific person
  private Map<String, FamilyRelation> findFamilyMapForPerson(
      Map<Person, Map<String, FamilyRelation>> familyMap,
      Person person) {
    return familyMap.entrySet().stream()
        .filter(entry -> entry.getKey().equals(person))
        .map(Map.Entry::getValue)
        .findFirst()
        .orElse(null);
  }

  /// Test complex nested map with Person keys and FamilyRelation values
  @Test
  void testNestedFamilyMap() {
    // Create test data
    final var familyData = createFamilyTestData();
    final var john = new Person("John", 40);
    final var emma = new Person("Emma", 38);

    // Create a record containing the nested map structure
    final var original = new NestedFamilyMapContainer(familyData);

    // Serialize and deserialize
    final var deserialized = serializeAndDeserialize(original);

    // Verify outer map size
    assertEquals(original.familyMap().size(), deserialized.familyMap().size(), "Outer map size mismatch");

    // Find the family maps for each person in the deserialized result
    final var deserializedFamily1 = findFamilyMapForPerson(deserialized.familyMap(), john);
    final var deserializedFamily2 = findFamilyMapForPerson(deserialized.familyMap(), emma);

    // Get original family maps for comparison
    final var originalFamily1 = findFamilyMapForPerson(original.familyMap(), john);
    final var originalFamily2 = findFamilyMapForPerson(original.familyMap(), emma);

    // Verify the nested maps were properly deserialized
    assertNotNull(deserializedFamily1, "Family map for John not found");
    assertNotNull(deserializedFamily2, "Family map for Emma not found");

    // Verify John's family contents
    assertEquals(originalFamily1.size(), deserializedFamily1.size(), "John's family map size mismatch");
    verifyFamilyRelation(originalFamily1.get("father"), deserializedFamily1.get("father"), "father");
    verifyFamilyRelation(originalFamily1.get("mother"), deserializedFamily1.get("mother"), "mother");

    // Verify Emma's family contents
    assertEquals(originalFamily2.size(), deserializedFamily2.size(), "Emma's family map size mismatch");
    verifyFamilyRelation(originalFamily2.get("sister"), deserializedFamily2.get("sister"), "sister");
  }

  /// Helper method to verify a family relation
  private void verifyFamilyRelation(FamilyRelation expected, FamilyRelation actual, String relationName) {
    assertEquals(expected.relationship(), actual.relationship(),
        relationName + " relationship type mismatch");
    assertEquals(expected.member(), actual.member(),
        relationName + " member mismatch");
  }
}
