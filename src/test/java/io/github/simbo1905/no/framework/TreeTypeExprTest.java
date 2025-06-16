package io.github.simbo1905.no.framework;

import static io.github.simbo1905.no.framework.Pickler.LOGGER;

import org.junit.jupiter.api.*;
import java.util.*;
import java.lang.reflect.Type;

import static org.junit.jupiter.api.Assertions.*;

/// Test suite for the new tree-based TypeExpr structure
/// Following TDD: RED phase - these tests define expected behavior
class TreeTypeExprTest {

    @BeforeEach
    void setUp() {
        LOGGER.fine(() -> "Starting tree TypeExpr test");
    }

    @AfterEach  
    void tearDown() {
        LOGGER.fine(() -> "Finished tree TypeExpr test");
    }
    
    @Nested
    @DisplayName("Primitive Type Tests")
    class PrimitiveTypeTests {
        
        @Test
        @DisplayName("Boolean primitive")
        void testBooleanPrimitive() {
            TypeExpr node = TypeExpr.analyze(boolean.class);
            
            assertInstanceOf(TypeExpr.PrimitiveNode.class, node);
            TypeExpr.PrimitiveNode prim = (TypeExpr.PrimitiveNode) node;
            assertEquals(TypeExpr.PrimitiveType.BOOLEAN, prim.type());
            assertEquals(boolean.class, prim.javaType());
            assertEquals("BOOLEAN", node.toTreeString());
        }
        
        @Test
        @DisplayName("Integer primitive")
        void testIntegerPrimitive() {
            TypeExpr node = TypeExpr.analyze(int.class);
            
            assertInstanceOf(TypeExpr.PrimitiveNode.class, node);
            TypeExpr.PrimitiveNode prim = (TypeExpr.PrimitiveNode) node;
            assertEquals(TypeExpr.PrimitiveType.INTEGER, prim.type());
            assertEquals(int.class, prim.javaType());
            assertEquals("INTEGER", node.toTreeString());
        }
        
        @Test
        @DisplayName("String type")
        void testStringType() {
            TypeExpr node = TypeExpr.analyze(String.class);
            
            assertInstanceOf(TypeExpr.PrimitiveNode.class, node);
            TypeExpr.PrimitiveNode prim = (TypeExpr.PrimitiveNode) node;
            assertEquals(TypeExpr.PrimitiveType.STRING, prim.type());
            assertEquals(String.class, prim.javaType());
            assertEquals("STRING", node.toTreeString());
        }
        
        @Test
        @DisplayName("UUID type")
        void testUUIDType() {
            TypeExpr node = TypeExpr.analyze(UUID.class);
            
            assertInstanceOf(TypeExpr.PrimitiveNode.class, node);
            TypeExpr.PrimitiveNode prim = (TypeExpr.PrimitiveNode) node;
            assertEquals(TypeExpr.PrimitiveType.UUID, prim.type());
            assertEquals(UUID.class, prim.javaType());
            assertEquals("UUID", node.toTreeString());
        }
    }
    
    @Nested
    @DisplayName("Simple Container Tests")  
    class SimpleContainerTests {
        
        @Test
        @DisplayName("Array of int")
        void testIntArray() {
            TypeExpr node = TypeExpr.analyze(int[].class);
            
            assertInstanceOf(TypeExpr.ArrayNode.class, node);
            TypeExpr.ArrayNode array = (TypeExpr.ArrayNode) node;
            
            // Check element type
            assertInstanceOf(TypeExpr.PrimitiveNode.class, array.element());
            TypeExpr.PrimitiveNode elem = (TypeExpr.PrimitiveNode) array.element();
            assertEquals(TypeExpr.PrimitiveType.INTEGER, elem.type());
            
            assertEquals("ARRAY(INTEGER)", node.toTreeString());
        }
        
        @Test
        @DisplayName("Array of String")
        void testStringArray() {
            TypeExpr node = TypeExpr.analyze(String[].class);
            
            assertInstanceOf(TypeExpr.ArrayNode.class, node);
            TypeExpr.ArrayNode array = (TypeExpr.ArrayNode) node;
            
            assertInstanceOf(TypeExpr.PrimitiveNode.class, array.element());
            TypeExpr.PrimitiveNode elem = (TypeExpr.PrimitiveNode) array.element();
            assertEquals(TypeExpr.PrimitiveType.STRING, elem.type());
            
            assertEquals("ARRAY(STRING)", node.toTreeString());
        }
        
        @Test
        @DisplayName("List of String")
        void testListString() throws Exception {
            Type listType = TreeTypeExprTest.class.getDeclaredField("stringList").getGenericType();
            TypeExpr node = TypeExpr.analyze(listType);
            
            assertInstanceOf(TypeExpr.ListNode.class, node);
            TypeExpr.ListNode list = (TypeExpr.ListNode) node;
            
            assertInstanceOf(TypeExpr.PrimitiveNode.class, list.element());
            TypeExpr.PrimitiveNode elem = (TypeExpr.PrimitiveNode) list.element();
            assertEquals(TypeExpr.PrimitiveType.STRING, elem.type());
            
            assertEquals("LIST(STRING)", node.toTreeString());
        }
        
        @Test
        @DisplayName("Optional of Integer")
        void testOptionalInteger() throws Exception {
            Type optType = TreeTypeExprTest.class.getDeclaredField("optionalInt").getGenericType();
            TypeExpr node = TypeExpr.analyze(optType);
            
            assertInstanceOf(TypeExpr.OptionalNode.class, node);
            TypeExpr.OptionalNode opt = (TypeExpr.OptionalNode) node;
            
            assertInstanceOf(TypeExpr.PrimitiveNode.class, opt.wrapped());
            TypeExpr.PrimitiveNode wrapped = (TypeExpr.PrimitiveNode) opt.wrapped();
            assertEquals(TypeExpr.PrimitiveType.INTEGER, wrapped.type());
            
            assertEquals("OPTIONAL(INTEGER)", node.toTreeString());
        }
    }
    
    @Nested
    @DisplayName("Map Tests")
    class MapTests {
        
        @Test
        @DisplayName("Map String to Integer")
        void testMapStringInteger() throws Exception {
            Type mapType = TreeTypeExprTest.class.getDeclaredField("stringIntMap").getGenericType();
            TypeExpr node = TypeExpr.analyze(mapType);
            
            assertInstanceOf(TypeExpr.MapNode.class, node);
            TypeExpr.MapNode map = (TypeExpr.MapNode) node;
            
            // Check key type
            assertInstanceOf(TypeExpr.PrimitiveNode.class, map.key());
            TypeExpr.PrimitiveNode key = (TypeExpr.PrimitiveNode) map.key();
            assertEquals(TypeExpr.PrimitiveType.STRING, key.type());
            
            // Check value type
            assertInstanceOf(TypeExpr.PrimitiveNode.class, map.value());
            TypeExpr.PrimitiveNode value = (TypeExpr.PrimitiveNode) map.value();
            assertEquals(TypeExpr.PrimitiveType.INTEGER, value.type());
            
            assertEquals("MAP(STRING, INTEGER)", node.toTreeString());
        }
    }
    
    @Nested
    @DisplayName("Nested Container Tests")
    class NestedContainerTests {
        
        @Test
        @DisplayName("List of Optional String")
        void testListOptionalString() throws Exception {
            Type type = TreeTypeExprTest.class.getDeclaredField("listOptionalString").getGenericType();
            TypeExpr node = TypeExpr.analyze(type);
            
            assertInstanceOf(TypeExpr.ListNode.class, node);
            TypeExpr.ListNode list = (TypeExpr.ListNode) node;
            
            assertInstanceOf(TypeExpr.OptionalNode.class, list.element());
            TypeExpr.OptionalNode opt = (TypeExpr.OptionalNode) list.element();
            
            assertInstanceOf(TypeExpr.PrimitiveNode.class, opt.wrapped());
            TypeExpr.PrimitiveNode str = (TypeExpr.PrimitiveNode) opt.wrapped();
            assertEquals(TypeExpr.PrimitiveType.STRING, str.type());
            
            assertEquals("LIST(OPTIONAL(STRING))", node.toTreeString());
        }
        
        @Test
        @DisplayName("Map with List values")
        void testMapStringListInteger() throws Exception {
            Type type = TreeTypeExprTest.class.getDeclaredField("mapStringListInt").getGenericType();
            TypeExpr node = TypeExpr.analyze(type);
            
            assertInstanceOf(TypeExpr.MapNode.class, node);
            TypeExpr.MapNode map = (TypeExpr.MapNode) node;
            
            // Key is String
            assertInstanceOf(TypeExpr.PrimitiveNode.class, map.key());
            assertEquals(TypeExpr.PrimitiveType.STRING, ((TypeExpr.PrimitiveNode) map.key()).type());
            
            // Value is List<Integer>
            assertInstanceOf(TypeExpr.ListNode.class, map.value());
            TypeExpr.ListNode list = (TypeExpr.ListNode) map.value();
            assertInstanceOf(TypeExpr.PrimitiveNode.class, list.element());
            assertEquals(TypeExpr.PrimitiveType.INTEGER, ((TypeExpr.PrimitiveNode) list.element()).type());
            
            assertEquals("MAP(STRING, LIST(INTEGER))", node.toTreeString());
        }
    }
    
    @Nested
    @DisplayName("User-Defined Type Tests")
    class UserDefinedTypeTests {
        
        @Test
        @DisplayName("Enum type")
        void testEnumType() {
            TypeExpr node = TypeExpr.analyze(TestEnum.class);
            
            assertInstanceOf(TypeExpr.PrimitiveNode.class, node);
            TypeExpr.PrimitiveNode prim = (TypeExpr.PrimitiveNode) node;
            assertEquals(TypeExpr.PrimitiveType.ENUM, prim.type());
            assertEquals(TestEnum.class, prim.javaType());
            assertEquals("ENUM", node.toTreeString());
        }
        
        @Test
        @DisplayName("Record type")
        void testRecordType() {
            TypeExpr node = TypeExpr.analyze(TestRecord.class);
            
            assertInstanceOf(TypeExpr.PrimitiveNode.class, node);
            TypeExpr.PrimitiveNode prim = (TypeExpr.PrimitiveNode) node;
            assertEquals(TypeExpr.PrimitiveType.RECORD, prim.type());
            assertEquals(TestRecord.class, prim.javaType());
            assertEquals("RECORD", node.toTreeString());
        }
    }
    
    // Test types for reflection
    List<String> stringList;
    Optional<Integer> optionalInt;
    Map<String, Integer> stringIntMap;
    List<Optional<String>> listOptionalString;
    Map<String, List<Integer>> mapStringListInt;
    
    enum TestEnum { ONE, TWO }
    record TestRecord(String name, int value) {}

  @Nested
  @DisplayName("Readme Specific Type Tests")
  class ReadmeSpecificExamplesTest {

    @BeforeEach
    void setUp() {
      // Cache not implemented in TypeExpr
    }

    @AfterEach
    void tearDown() {
      // Cache not implemented in TypeExpr
    }

    TypeExpr intType = TypeExpr.analyze(int.class);

    TypeExpr booleanType = TypeExpr.analyze(boolean.class);

    // ===============================
    // Example 1: Basic Record and Enum (Month/Season)
    // ===============================

    public record Month(Season season, String name) {
    }

    public enum Season {SPRING, SUMMER, FALL, WINTER}

    @Test
    @DisplayName("README Example: Month Record with Season Enum")
    void testMonthSeasonExample() {
      // Test the Season enum directly
      TypeExpr seasonEnum = TypeExpr.analyze(Season.class);
      assertEquals(new TypeExpr.PrimitiveNode(TypeExpr.PrimitiveType.ENUM, Season.class), seasonEnum);

      // Test String type
      TypeExpr stringType = TypeExpr.analyze(String.class);
      assertEquals(new TypeExpr.PrimitiveNode(TypeExpr.PrimitiveType.STRING, String.class), stringType);
    }

    // ===============================
    // Example 2: TreeNode Sealed Interface Hierarchy
    // ===============================

    public sealed interface TreeNode permits InternalNode, LeafNode, TreeEnum {
      static TreeNode empty() {
        return TreeEnum.EMPTY;
      }
    }

    public record InternalNode(String name, TreeNode left, TreeNode right) implements TreeNode {
    }

    public record LeafNode(int value) implements TreeNode {
    }

    public enum TreeEnum implements TreeNode {EMPTY}

    @Test
    @DisplayName("README Example: TreeNode Sealed Interface")
    void testTreeNodeSealedInterface() {
      // Test the sealed interface itself
      TypeExpr treeNodeInterface = TypeExpr.analyze(TreeNode.class);
      assertEquals(new TypeExpr.PrimitiveNode(TypeExpr.PrimitiveType.INTERFACE, TreeNode.class), treeNodeInterface);

      // Test record implementing interface
      TypeExpr internalNodeRecord = TypeExpr.analyze(InternalNode.class);
      assertEquals(new TypeExpr.PrimitiveNode(TypeExpr.PrimitiveType.RECORD, InternalNode.class), internalNodeRecord);

      // Test enum implementing interface
      TypeExpr treeEnum = TypeExpr.analyze(TreeEnum.class);
      assertEquals(new TypeExpr.PrimitiveNode(TypeExpr.PrimitiveType.ENUM, TreeEnum.class), treeEnum);
    }

    @Test
    @DisplayName("README Example: InternalNode Record Structure")
    void testInternalNodeStructure() {
      // Test the types of fields in InternalNode
      // name: String
      TypeExpr stringType = TypeExpr.analyze(String.class);
      assertEquals(new TypeExpr.PrimitiveNode(TypeExpr.PrimitiveType.STRING, String.class), stringType);

      // left/right: TreeNode (recursive reference to sealed interface)
      TypeExpr treeNodeType = TypeExpr.analyze(TreeNode.class);
      assertEquals(new TypeExpr.PrimitiveNode(TypeExpr.PrimitiveType.INTERFACE, TreeNode.class), treeNodeType);
    }

    @Test
    @DisplayName("README Example: LeafNode Record Structure")
    void testLeafNodeStructure() {
      // Test int type for value field
      TypeExpr intType = TypeExpr.analyze(int.class);
      assertEquals(new TypeExpr.PrimitiveNode(TypeExpr.PrimitiveType.INTEGER, int.class), intType);
    }

    // ===============================
    // Example 3: Nested List Records (NestedListRecord)
    // ===============================

    public record NestedListRecord(List<List<String>> nestedList) {
    }

    @Test
    @DisplayName("README Example: NestedListRecord Structure")
    void testNestedListRecordStructure() throws Exception {
      Type nestedListType = NestedListRecord.class.getDeclaredField("nestedList").getGenericType();

      // Build expected tree: LIST(LIST(STRING))
      TypeExpr expected = new TypeExpr.ListNode(
          new TypeExpr.ListNode(
              new TypeExpr.PrimitiveNode(TypeExpr.PrimitiveType.STRING, String.class)
          )
      );

      TypeExpr actual = TypeExpr.analyze(nestedListType);
      assertEquals(expected, actual);
      assertEquals("LIST(LIST(STRING))", actual.toTreeString());
    }

    // ===============================
    // Example 4: Person and Family Map Records
    // ===============================

    public record Person(String name, int age) {
    }

    public record NestedFamilyMapContainer(Person subject, Map<String, Person> relationships) {
    }

    @Test
    @DisplayName("README Example: Person Record Structure")
    void testPersonRecordStructure() {
      // Test the types used in Person record
      TypeExpr stringType = TypeExpr.analyze(String.class);
      assertEquals(new TypeExpr.PrimitiveNode(TypeExpr.PrimitiveType.STRING, String.class), stringType);

      TypeExpr intType = TypeExpr.analyze(int.class);
      assertEquals(new TypeExpr.PrimitiveNode(TypeExpr.PrimitiveType.INTEGER, int.class), intType);
    }

    @Test
    @DisplayName("README Example: NestedFamilyMapContainer Structure")
    void testNestedFamilyMapContainerStructure() throws Exception {
      // subject: Person (record)
      TypeExpr personType = TypeExpr.analyze(Person.class);
      assertEquals(new TypeExpr.PrimitiveNode(TypeExpr.PrimitiveType.RECORD, Person.class), personType);

      // relationships: Map<String, Person>
      Type mapType = NestedFamilyMapContainer.class.getDeclaredField("relationships").getGenericType();
      TypeExpr expected = new TypeExpr.MapNode(
          new TypeExpr.PrimitiveNode(TypeExpr.PrimitiveType.STRING, String.class),
          new TypeExpr.PrimitiveNode(TypeExpr.PrimitiveType.RECORD, Person.class)
      );
      TypeExpr actual = TypeExpr.analyze(mapType);
      assertEquals(expected, actual);
      assertEquals("MAP(STRING, RECORD)", actual.toTreeString());
    }

    // ===============================
    // Example 5: Complex Animal Hierarchy
    // ===============================

    public sealed interface Animal permits Mammal, Bird, Alicorn {
    }

    public sealed interface Mammal extends Animal permits Dog, Cat {
    }

    public sealed interface Bird extends Animal permits Eagle, Penguin {
    }

    public record Alicorn(String name, String[] magicPowers) implements Animal {
    }

    public record Dog(String name, int age) implements Mammal {
    }

    public record Cat(String name, boolean purrs) implements Mammal {
    }

    public record Eagle(double wingspan) implements Bird {
    }

    public record Penguin(boolean canSwim) implements Bird {
    }

    @Test
    @DisplayName("README Example: Animal Sealed Hierarchy Analysis")
    void testAnimalSealedHierarchy() {
      // Test various types in the hierarchy
      TypeExpr alicornType = TypeExpr.analyze(Alicorn.class);
      assertEquals(new TypeExpr.PrimitiveNode(TypeExpr.PrimitiveType.RECORD, Alicorn.class), alicornType);

      TypeExpr mammalType = TypeExpr.analyze(Mammal.class);
      assertEquals(new TypeExpr.PrimitiveNode(TypeExpr.PrimitiveType.INTERFACE, Mammal.class), mammalType);

      TypeExpr dogType = TypeExpr.analyze(Dog.class);
      assertEquals(new TypeExpr.PrimitiveNode(TypeExpr.PrimitiveType.RECORD, Dog.class), dogType);
    }

    @Test
    @DisplayName("README Example: Alicorn Record with String Array")
    void testAlicornRecordStructure() throws Exception {
      // name: String
      TypeExpr stringType = TypeExpr.analyze(String.class);
      assertEquals(new TypeExpr.PrimitiveNode(TypeExpr.PrimitiveType.STRING, String.class), stringType);

      // magicPowers: String[]
      Type arrayType = Alicorn.class.getDeclaredField("magicPowers").getGenericType();
      TypeExpr expected = new TypeExpr.ArrayNode(
          new TypeExpr.PrimitiveNode(TypeExpr.PrimitiveType.STRING, String.class)
      );
      TypeExpr actual = TypeExpr.analyze(arrayType);
      assertEquals(expected, actual);
      assertEquals("ARRAY(STRING)", actual.toTreeString());
    }

    @Test
    @DisplayName("README Example: Dog and Cat Record Structures")
    void testMammalRecordStructures() {
      // Test types used in Dog and Cat records
      TypeExpr stringType = TypeExpr.analyze(String.class);
      assertEquals(new TypeExpr.PrimitiveNode(TypeExpr.PrimitiveType.STRING, String.class), stringType);
      assertEquals(new TypeExpr.PrimitiveNode(TypeExpr.PrimitiveType.INTEGER, int.class), intType);
      assertEquals(new TypeExpr.PrimitiveNode(TypeExpr.PrimitiveType.BOOLEAN, boolean.class), booleanType);
    }

    @Test
    @DisplayName("README Example: Eagle and Penguin Record Structures")
    void testBirdRecordStructures() {
      // Test types used in Eagle and Penguin records
      TypeExpr doubleType = TypeExpr.analyze(double.class);
      assertEquals(new TypeExpr.PrimitiveNode(TypeExpr.PrimitiveType.DOUBLE, double.class), doubleType);

      TypeExpr booleanType = TypeExpr.analyze(boolean.class);
      assertEquals(new TypeExpr.PrimitiveNode(TypeExpr.PrimitiveType.BOOLEAN, boolean.class), booleanType);
    }

    // ===============================
    // Example 6: Complex Type from Academic Analysis
    // List<Map<String, Optional<Integer[]>[]>>
    // ===============================

    @Test
    @DisplayName("README Academic Example: List<Map<String, Optional<Integer[]>[]>>")
    void testComplexAcademicExample() throws Exception {
      Type complexType = getClass().getDeclaredField("complexAcademicExample").getGenericType();

      // Build expected tree from inside out: LIST(MAP(STRING, ARRAY(OPTIONAL(ARRAY(INTEGER)))))
      TypeExpr integerNode = new TypeExpr.PrimitiveNode(TypeExpr.PrimitiveType.INTEGER, Integer.class);
      TypeExpr innerArrayNode = new TypeExpr.ArrayNode(integerNode);
      TypeExpr optionalNode = new TypeExpr.OptionalNode(innerArrayNode);
      TypeExpr outerArrayNode = new TypeExpr.ArrayNode(optionalNode);
      TypeExpr stringNode = new TypeExpr.PrimitiveNode(TypeExpr.PrimitiveType.STRING, String.class);
      TypeExpr mapNode = new TypeExpr.MapNode(stringNode, outerArrayNode);
      TypeExpr expected = new TypeExpr.ListNode(mapNode);

      TypeExpr actual = TypeExpr.analyze(complexType);
      assertEquals(expected, actual);
      assertEquals("LIST(MAP(STRING, ARRAY(OPTIONAL(ARRAY(INTEGER)))))", actual.toTreeString());
    }

    // ===============================
    // Example 7: Wire Protocol Supported Types
    // ===============================

    @Test
    @DisplayName("README Wire Protocol: All Primitive Types")
    void testWireProtocolPrimitiveTypes() {
      // Map each type to its expected TypeExpr.PrimitiveType
      Map<Class<?>, TypeExpr.PrimitiveType> typeMapping = Map.ofEntries(
          Map.entry(boolean.class, TypeExpr.PrimitiveType.BOOLEAN),
          Map.entry(Boolean.class, TypeExpr.PrimitiveType.BOOLEAN),
          Map.entry(byte.class, TypeExpr.PrimitiveType.BYTE),
          Map.entry(Byte.class, TypeExpr.PrimitiveType.BYTE),
          Map.entry(short.class, TypeExpr.PrimitiveType.SHORT),
          Map.entry(Short.class, TypeExpr.PrimitiveType.SHORT),
          Map.entry(char.class, TypeExpr.PrimitiveType.CHARACTER),
          Map.entry(Character.class, TypeExpr.PrimitiveType.CHARACTER),
          Map.entry(int.class, TypeExpr.PrimitiveType.INTEGER),
          Map.entry(Integer.class, TypeExpr.PrimitiveType.INTEGER),
          Map.entry(long.class, TypeExpr.PrimitiveType.LONG),
          Map.entry(Long.class, TypeExpr.PrimitiveType.LONG),
          Map.entry(float.class, TypeExpr.PrimitiveType.FLOAT),
          Map.entry(Float.class, TypeExpr.PrimitiveType.FLOAT),
          Map.entry(double.class, TypeExpr.PrimitiveType.DOUBLE),
          Map.entry(Double.class, TypeExpr.PrimitiveType.DOUBLE),
          Map.entry(String.class, TypeExpr.PrimitiveType.STRING),
          Map.entry(UUID.class, TypeExpr.PrimitiveType.UUID)
      );

      for (Map.Entry<Class<?>, TypeExpr.PrimitiveType> entry : typeMapping.entrySet()) {
        Class<?> javaType = entry.getKey();
        TypeExpr.PrimitiveType expectedType = entry.getValue();

        TypeExpr expected = new TypeExpr.PrimitiveNode(expectedType, javaType);
        TypeExpr actual = TypeExpr.analyze(javaType);

        assertEquals(expected, actual, "Failed for " + javaType);
        assertEquals(expectedType.name(), actual.toTreeString());
      }
    }

    @Test
    @DisplayName("README Wire Protocol: Container Types")
    void testWireProtocolContainerTypes() throws Exception {
      // Test Array: int[] -> ARRAY(INTEGER)
      TypeExpr expectedArray = new TypeExpr.ArrayNode(
          new TypeExpr.PrimitiveNode(TypeExpr.PrimitiveType.INTEGER, int.class)
      );
      TypeExpr actualArray = TypeExpr.analyze(int[].class);
      assertEquals(expectedArray, actualArray);
      assertEquals("ARRAY(INTEGER)", actualArray.toTreeString());

      // Test List: List<String> -> LIST(STRING)
      Type listType = getClass().getDeclaredField("stringList").getGenericType();
      TypeExpr expectedList = new TypeExpr.ListNode(
          new TypeExpr.PrimitiveNode(TypeExpr.PrimitiveType.STRING, String.class)
      );
      TypeExpr actualList = TypeExpr.analyze(listType);
      assertEquals(expectedList, actualList);
      assertEquals("LIST(STRING)", actualList.toTreeString());

      // Test Optional: Optional<String> -> OPTIONAL(STRING)
      Type optionalType = getClass().getDeclaredField("optionalString").getGenericType();
      TypeExpr expectedOptional = new TypeExpr.OptionalNode(
          new TypeExpr.PrimitiveNode(TypeExpr.PrimitiveType.STRING, String.class)
      );
      TypeExpr actualOptional = TypeExpr.analyze(optionalType);
      assertEquals(expectedOptional, actualOptional);
      assertEquals("OPTIONAL(STRING)", actualOptional.toTreeString());

      // Test Map: Map<String, Integer> -> MAP(STRING, INTEGER)
      Type mapType = getClass().getDeclaredField("stringIntMap").getGenericType();
      TypeExpr expectedMap = new TypeExpr.MapNode(
          new TypeExpr.PrimitiveNode(TypeExpr.PrimitiveType.STRING, String.class),
          new TypeExpr.PrimitiveNode(TypeExpr.PrimitiveType.INTEGER, Integer.class)
      );
      TypeExpr actualMap = TypeExpr.analyze(mapType);
      assertEquals(expectedMap, actualMap);
      assertEquals("MAP(STRING, INTEGER)", actualMap.toTreeString());
    }


    // Required fields for reflection-based generic type access
    @SuppressWarnings("unused")
    List<Map<String, Optional<Integer[]>[]>> complexAcademicExample;
    @SuppressWarnings("unused")
    List<String> stringList;
    @SuppressWarnings("unused")
    Optional<String> optionalString;
    @SuppressWarnings("unused")
    Map<String, Integer> stringIntMap;
  }

}
