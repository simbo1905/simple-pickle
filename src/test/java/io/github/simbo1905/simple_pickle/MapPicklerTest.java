// SPDX-FileCopyrightText: 2025 Simon Massey
// SPDX-License-Identifier: Apache-2.0
package io.github.simbo1905.simple_pickle;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.ConsoleHandler;
import java.util.logging.Level;
import java.util.logging.Logger;

import static io.github.simbo1905.simple_pickle.Pickler.picklerForRecord;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for Map serialization and deserialization.
 */
class MapPicklerTest {

    private static final Logger LOGGER = Logger.getLogger(MapPicklerTest.class.getName());

    static {
        // Set up logging
        LOGGER.setLevel(Level.FINE);
        ConsoleHandler handler = new ConsoleHandler();
        handler.setLevel(Level.FINE);
        LOGGER.addHandler(handler);
    }
    
    /**
     * Simple Person record for testing.
     */
    record Person(String name, int age) {}
    
    /**
     * FamilyRelation record for testing nested record serialization.
     */
    record FamilyRelation(String relationship, Person member) {
    }

    /**
     * Test record containing a Map field.
     */
    record MapContainer(Map<String, Integer> stringToInt) {}

    /**
     * Test record containing a nested Map field.
     */
    record NestedMapContainer(Map<String, Map<Integer, String>> nestedMap) {}

    /**
     * Test record containing a Map with record values.
     */
    record RecordMapContainer(Map<String, Person> personMap) {}
    
    /**
     * Test record containing a Map with Person keys and nested Map values.
     */
    record NestedFamilyMapContainer(Map<Person, Map<String, FamilyRelation>> familyMap) {
    }

    /**
     * Test basic Map serialization and deserialization.
     */
    @Test
    void testBasicMap() {
        // Create a map
        Map<String, Integer> map = new HashMap<>();
        map.put("one", 1);
        map.put("two", 2);
        map.put("three", 3);
        
        // Create a record containing the map
        MapContainer original = new MapContainer(map);
        
        // Get a pickler for the record
        Pickler<MapContainer> pickler = picklerForRecord(MapContainer.class);
        
        // Calculate size and allocate buffer
        int size = pickler.sizeOf(original);
        ByteBuffer buffer = ByteBuffer.allocate(size);
        
        // Serialize
        pickler.serialize(original, buffer);
        buffer.flip();
        
        // Deserialize
        MapContainer deserialized = pickler.deserialize(buffer);
        
        // Verify map content
        assertEquals(original.stringToInt().size(), deserialized.stringToInt().size());
        assertEquals(original.stringToInt().get("one"), deserialized.stringToInt().get("one"));
        assertEquals(original.stringToInt().get("two"), deserialized.stringToInt().get("two"));
        assertEquals(original.stringToInt().get("three"), deserialized.stringToInt().get("three"));
        
        // Verify buffer is fully consumed
        assertEquals(buffer.limit(), buffer.position());
    }
    
    /**
     * Test empty Map serialization and deserialization.
     */
    @Test
    void testEmptyMap() {
        // Create an empty map
        Map<String, Integer> emptyMap = new HashMap<>();
        
        // Create a record containing the empty map
        MapContainer original = new MapContainer(emptyMap);
        
        // Get a pickler for the record
        Pickler<MapContainer> pickler = picklerForRecord(MapContainer.class);
        
        // Calculate size and allocate buffer
        int size = pickler.sizeOf(original);
        ByteBuffer buffer = ByteBuffer.allocate(size);
        
        // Serialize
        pickler.serialize(original, buffer);
        buffer.flip();
        
        // Deserialize
        MapContainer deserialized = pickler.deserialize(buffer);
        
        // Verify map is empty
        assertTrue(deserialized.stringToInt().isEmpty());
        
        // Verify buffer is fully consumed
        assertEquals(buffer.limit(), buffer.position());
    }
    
    /**
     * Test null Map serialization and deserialization.
     */
    @Test
    void testNullMap() {
        // Create a record with a null map
        MapContainer original = new MapContainer(null);
        
        // Get a pickler for the record
        Pickler<MapContainer> pickler = picklerForRecord(MapContainer.class);
        
        // Calculate size and allocate buffer
        int size = pickler.sizeOf(original);
        ByteBuffer buffer = ByteBuffer.allocate(size);
        
        // Serialize
        pickler.serialize(original, buffer);
        buffer.flip();
        
        // Deserialize
        MapContainer deserialized = pickler.deserialize(buffer);
        
        // Verify map is null
        assertNull(deserialized.stringToInt());
        
        // Verify buffer is fully consumed
        assertEquals(buffer.limit(), buffer.position());
    }
    
    /**
     * Test nested Map serialization and deserialization.
     */
    @Test
    void testNestedMap() {
        // Create a nested map
        Map<String, Map<Integer, String>> nestedMap = new HashMap<>();
        
        Map<Integer, String> innerMap1 = new HashMap<>();
        innerMap1.put(1, "one");
        innerMap1.put(2, "two");
        
        Map<Integer, String> innerMap2 = new HashMap<>();
        innerMap2.put(3, "three");
        innerMap2.put(4, "four");
        
        nestedMap.put("first", innerMap1);
        nestedMap.put("second", innerMap2);
        
        // Create a record containing the nested map
        NestedMapContainer original = new NestedMapContainer(nestedMap);
        
        // Get a pickler for the record
        Pickler<NestedMapContainer> pickler = picklerForRecord(NestedMapContainer.class);
        
        // Calculate size and allocate buffer
        int size = pickler.sizeOf(original);
        ByteBuffer buffer = ByteBuffer.allocate(size);
        
        // Serialize
        pickler.serialize(original, buffer);
        buffer.flip();
        
        // Deserialize
        NestedMapContainer deserialized = pickler.deserialize(buffer);
        
        // Verify nested map content
        assertEquals(original.nestedMap().size(), deserialized.nestedMap().size());
        
        Map<Integer, String> deserializedInnerMap1 = deserialized.nestedMap().get("first");
        assertEquals(innerMap1.size(), deserializedInnerMap1.size());
        assertEquals(innerMap1.get(1), deserializedInnerMap1.get(1));
        assertEquals(innerMap1.get(2), deserializedInnerMap1.get(2));
        
        Map<Integer, String> deserializedInnerMap2 = deserialized.nestedMap().get("second");
        assertEquals(innerMap2.size(), deserializedInnerMap2.size());
        assertEquals(innerMap2.get(3), deserializedInnerMap2.get(3));
        assertEquals(innerMap2.get(4), deserializedInnerMap2.get(4));
        
        // Verify buffer is fully consumed
        assertEquals(buffer.limit(), buffer.position());
    }
    
    /**
     * Test Map with record values serialization and deserialization.
     */
    @Test
    void testMapWithRecordValues() {
        // Create a map with record values
        Map<String, Person> personMap = new HashMap<>();
        personMap.put("alice", new Person("Alice", 30));
        personMap.put("bob", new Person("Bob", 25));
        
        // Create a record containing the map
        RecordMapContainer original = new RecordMapContainer(personMap);
        
        // Get a pickler for the record
        Pickler<RecordMapContainer> pickler = picklerForRecord(RecordMapContainer.class);
        
        // Calculate size and allocate buffer
        int size = pickler.sizeOf(original);
        ByteBuffer buffer = ByteBuffer.allocate(size);
        
        // Serialize
        pickler.serialize(original, buffer);
        buffer.flip();
        
        // Deserialize
        RecordMapContainer deserialized = pickler.deserialize(buffer);
        
        // Verify map content
        assertEquals(original.personMap().size(), deserialized.personMap().size());
        assertEquals(original.personMap().get("alice"), deserialized.personMap().get("alice"));
        assertEquals(original.personMap().get("bob"), deserialized.personMap().get("bob"));
        
        // Verify buffer is fully consumed
        assertEquals(buffer.limit(), buffer.position());
    }

  @Test
  void testDocumentation() {
    Person john = new Person("John", 40);
    Person michael = new Person("Michael", 65);
    Person sarah = new Person("Sarah", 63);

    Map<String, Person> familyMap = new HashMap<>();
    familyMap.put("father", michael);
    familyMap.put("mother", sarah);

    record NestedFamilyMapContainer(Person subject, Map<String, Person> relationships) {
    }

    // we need a wrapper to create a concrete type for the pickler due to erase of map types
    final var nestedFamilyMapContainer = new NestedFamilyMapContainer(john, familyMap);

    final var pickler = picklerForRecord(NestedFamilyMapContainer.class);

    // Calculate size and allocate buffer
    int size = pickler.sizeOf(nestedFamilyMapContainer);
    ByteBuffer buffer = ByteBuffer.allocate(size);
    // Serialize
    pickler.serialize(nestedFamilyMapContainer, buffer);
    buffer.flip();
    // Deserialize
    NestedFamilyMapContainer deserialized = pickler.deserialize(buffer);
    // Verify map content
    assertEquals(nestedFamilyMapContainer.subject, deserialized.subject);
    assertEquals(nestedFamilyMapContainer.relationships.size(), deserialized.relationships.size());
    assertEquals(nestedFamilyMapContainer.relationships.get("father"), deserialized.relationships.get("father"));
    assertEquals(nestedFamilyMapContainer.relationships.get("mother"), deserialized.relationships.get("mother"));
    // Verify buffer is fully consumed
    assertEquals(buffer.limit(), buffer.position());

  }

  /**
   * Test Map with Person keys and nested FamilyRelation records serialization and deserialization.
     */
    @Test
    void testNestedFamilyMap() {
        // Create Person objects to use as keys
        Person person1 = new Person("John", 40);
        Person person2 = new Person("Michael", 65);
        Person person3 = new Person("Sarah", 63);
        Person person4 = new Person("Emma", 38);

      // Create FamilyRelation objects
      FamilyRelation father = new FamilyRelation("father", person2);
      FamilyRelation mother = new FamilyRelation("mother", person3);
      FamilyRelation sister = new FamilyRelation("sister", person4);
        
        // Create nested maps
      Map<String, FamilyRelation> family1 = new HashMap<>();
        family1.put("father", father);
        family1.put("mother", mother);

      Map<String, FamilyRelation> family2 = new HashMap<>();
        family2.put("sister", sister);
        
        // Create the outer map with Person keys
      Map<Person, Map<String, FamilyRelation>> familyMap = new HashMap<>();
        familyMap.put(person1, family1);
        familyMap.put(person4, family2);
        
        // Create a record containing the nested map structure
        NestedFamilyMapContainer original = new NestedFamilyMapContainer(familyMap);
        
        // Get a pickler for the record
        Pickler<NestedFamilyMapContainer> pickler = picklerForRecord(NestedFamilyMapContainer.class);
        
        // Calculate size and allocate buffer
        int size = pickler.sizeOf(original);
        ByteBuffer buffer = ByteBuffer.allocate(size);
        
        // Serialize
        pickler.serialize(original, buffer);
        buffer.flip();
        
        // Deserialize
        NestedFamilyMapContainer deserialized = pickler.deserialize(buffer);
        
        // Verify outer map size
        assertEquals(original.familyMap().size(), deserialized.familyMap().size());
        
        // Get the deserialized maps for each person
      Map<String, FamilyRelation> deserializedFamily1 = null;
      Map<String, FamilyRelation> deserializedFamily2 = null;
        
        // Find the corresponding maps in the deserialized result
        // We need to iterate because Person objects won't have the same reference
      for (Map.Entry<Person, Map<String, FamilyRelation>> entry : deserialized.familyMap().entrySet()) {
            if (entry.getKey().equals(person1)) {
                deserializedFamily1 = entry.getValue();
            } else if (entry.getKey().equals(person4)) {
                deserializedFamily2 = entry.getValue();
            }
        }
        
        // Verify the nested maps were properly deserialized
      assertNotNull(deserializedFamily1, "FamilyRelation map for person1 not found");
      assertNotNull(deserializedFamily2, "FamilyRelation map for person4 not found");
        
        // Verify family1 contents
        assertEquals(2, deserializedFamily1.size());
        assertEquals(father.relationship(), deserializedFamily1.get("father").relationship());
        assertEquals(father.member(), deserializedFamily1.get("father").member());
        assertEquals(mother.relationship(), deserializedFamily1.get("mother").relationship());
        assertEquals(mother.member(), deserializedFamily1.get("mother").member());
        
        // Verify family2 contents
        assertEquals(1, deserializedFamily2.size());
        assertEquals(sister.relationship(), deserializedFamily2.get("sister").relationship());
        assertEquals(sister.member(), deserializedFamily2.get("sister").member());
        
        // Verify buffer is fully consumed
        assertEquals(buffer.limit(), buffer.position());
    }
}
