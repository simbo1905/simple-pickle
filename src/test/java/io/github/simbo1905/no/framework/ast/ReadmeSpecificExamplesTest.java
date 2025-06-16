package io.github.simbo1905.no.framework.ast;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Type;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Specific test cases for examples mentioned in the README.md documentation.
 *
 * This test class focuses exclusively on validating that the AST construction
 * correctly handles all the specific examples provided in the README.md,
 * ensuring that the implementation matches the documented behavior.
 */
class ReadmeSpecificExamplesTest {

  @BeforeEach
  void setUp() {
    MetaStage.clearCache();
  }

  @AfterEach
  void tearDown() {
    MetaStage.clearCache();
  }

  // ===============================
  // Example 1: Basic Record and Enum (Month/Season)
  // ===============================

  public record Month(Season season, String name) {}
  public enum Season { SPRING, SUMMER, FALL, WINTER }

  @Test
  @DisplayName("README Example: Month Record with Season Enum")
  void testMonthSeasonExample() {
    // Test the Month record components
    Map<String, TypeStructureAST> monthComponents = MetaStage.analyzeRecordComponents(Month.class);

    assertEquals(2, monthComponents.size());
    assertTrue(monthComponents.containsKey("season"));
    assertTrue(monthComponents.containsKey("name"));

    // season component should be ENUM
    TypeStructureAST seasonAST = monthComponents.get("season");
    assertEquals(1, seasonAST.size());
    assertEquals(StructuralTag.ENUM, seasonAST.root().tag());
    assertEquals(Season.class, seasonAST.root().type());
    assertTrue(seasonAST.isSimple());

    // name component should be STRING
    TypeStructureAST nameAST = monthComponents.get("name");
    assertEquals(1, nameAST.size());
    assertEquals(StructuralTag.STRING, nameAST.root().tag());
    assertEquals(String.class, nameAST.root().type());
    assertTrue(nameAST.isSimple());

    // Test the Season enum directly
    TypeStructureAST seasonEnumAST = MetaStage.analyze(Season.class);
    assertEquals(1, seasonEnumAST.size());
    assertEquals(StructuralTag.ENUM, seasonEnumAST.root().tag());
    assertTrue(seasonEnumAST.isSimple());
  }

  // ===============================
  // Example 2: TreeNode Sealed Interface Hierarchy
  // ===============================

  public sealed interface TreeNode permits InternalNode, LeafNode, TreeEnum {
    static TreeNode empty() { return TreeEnum.EMPTY; }
  }

  public record InternalNode(String name, TreeNode left, TreeNode right) implements TreeNode {}
  public record LeafNode(int value) implements TreeNode {}
  public enum TreeEnum implements TreeNode { EMPTY }

  @Test
  @DisplayName("README Example: TreeNode Sealed Interface")
  void testTreeNodeSealedInterface() {
    // Test the sealed interface itself
    TypeStructureAST treeNodeAST = MetaStage.analyze(TreeNode.class);
    assertEquals(1, treeNodeAST.size());
    assertEquals(StructuralTag.INTERFACE, treeNodeAST.root().tag());
    assertEquals(TreeNode.class, treeNodeAST.root().type());
    assertTrue(treeNodeAST.isSimple());

    // Test sealed hierarchy analysis
    Map<Class<?>, TypeStructureAST> hierarchy = MetaStage.analyzeSealedHierarchy(TreeNode.class);
    assertEquals(3, hierarchy.size());
    assertTrue(hierarchy.containsKey(InternalNode.class));
    assertTrue(hierarchy.containsKey(LeafNode.class));
    assertTrue(hierarchy.containsKey(TreeEnum.class));
  }

  @Test
  @DisplayName("README Example: InternalNode Record Structure")
  void testInternalNodeStructure() {
    Map<String, TypeStructureAST> internalComponents = MetaStage.analyzeRecordComponents(InternalNode.class);

    assertEquals(3, internalComponents.size());
    assertTrue(internalComponents.containsKey("name"));
    assertTrue(internalComponents.containsKey("left"));
    assertTrue(internalComponents.containsKey("right"));

    // name: String
    TypeStructureAST nameAST = internalComponents.get("name");
    assertEquals(1, nameAST.size());
    assertEquals(StructuralTag.STRING, nameAST.root().tag());

    // left: TreeNode (recursive reference to sealed interface)
    TypeStructureAST leftAST = internalComponents.get("left");
    assertEquals(1, leftAST.size());
    assertEquals(StructuralTag.INTERFACE, leftAST.root().tag());
    assertEquals(TreeNode.class, leftAST.root().type());

    // right: TreeNode (recursive reference to sealed interface)
    TypeStructureAST rightAST = internalComponents.get("right");
    assertEquals(1, rightAST.size());
    assertEquals(StructuralTag.INTERFACE, rightAST.root().tag());
    assertEquals(TreeNode.class, rightAST.root().type());
  }

  @Test
  @DisplayName("README Example: LeafNode Record Structure")
  void testLeafNodeStructure() {
    Map<String, TypeStructureAST> leafComponents = MetaStage.analyzeRecordComponents(LeafNode.class);

    assertEquals(1, leafComponents.size());
    assertTrue(leafComponents.containsKey("value"));

    // value: int
    TypeStructureAST valueAST = leafComponents.get("value");
    assertEquals(1, valueAST.size());
    assertEquals(StructuralTag.INTEGER, valueAST.root().tag());
    assertEquals(int.class, valueAST.root().type());
  }

  // ===============================
  // Example 3: Nested List Records (NestedListRecord)
  // ===============================

  public record NestedListRecord(List<List<String>> nestedList) {}

  @Test
  @DisplayName("README Example: NestedListRecord Structure")
  void testNestedListRecordStructure() throws Exception {
    Map<String, TypeStructureAST> components = MetaStage.analyzeRecordComponents(NestedListRecord.class);

    assertEquals(1, components.size());
    assertTrue(components.containsKey("nestedList"));

    // nestedList: List<List<String>>
    TypeStructureAST nestedListAST = components.get("nestedList");
    assertEquals(3, nestedListAST.size()); // LIST, LIST, STRING

    assertEquals(StructuralTag.LIST, nestedListAST.get(0).tag());
    assertEquals(StructuralTag.LIST, nestedListAST.get(1).tag());
    assertEquals(StructuralTag.STRING, nestedListAST.get(2).tag());

    assertEquals("LIST<LIST<STRING>>", nestedListAST.toStructureString());
    assertTrue(nestedListAST.isNested());
    assertEquals(2, nestedListAST.containerCount());
  }

  // ===============================
  // Example 4: Person and Family Map Records
  // ===============================

  public record Person(String name, int age) {}
  public record NestedFamilyMapContainer(Person subject, Map<String, Person> relationships) {}

  @Test
  @DisplayName("README Example: Person Record Structure")
  void testPersonRecordStructure() {
    Map<String, TypeStructureAST> personComponents = MetaStage.analyzeRecordComponents(Person.class);

    assertEquals(2, personComponents.size());
    assertTrue(personComponents.containsKey("name"));
    assertTrue(personComponents.containsKey("age"));

    // name: String
    TypeStructureAST nameAST = personComponents.get("name");
    assertEquals(StructuralTag.STRING, nameAST.root().tag());

    // age: int
    TypeStructureAST ageAST = personComponents.get("age");
    assertEquals(StructuralTag.INTEGER, ageAST.root().tag());
  }

  @Test
  @DisplayName("README Example: NestedFamilyMapContainer Structure")
  void testNestedFamilyMapContainerStructure() {
    Map<String, TypeStructureAST> components = MetaStage.analyzeRecordComponents(NestedFamilyMapContainer.class);

    assertEquals(2, components.size());
    assertTrue(components.containsKey("subject"));
    assertTrue(components.containsKey("relationships"));

    // subject: Person (record)
    TypeStructureAST subjectAST = components.get("subject");
    assertEquals(1, subjectAST.size());
    assertEquals(StructuralTag.RECORD, subjectAST.root().tag());
    assertEquals(Person.class, subjectAST.root().type());

    // relationships: Map<String, Person>
    TypeStructureAST relationshipsAST = components.get("relationships");
    assertEquals(4, relationshipsAST.size()); // MAP, STRING, MAP_SEPARATOR, RECORD

    assertEquals(StructuralTag.MAP, relationshipsAST.get(0).tag());
    assertEquals(StructuralTag.STRING, relationshipsAST.get(1).tag());
    assertEquals(StructuralTag.MAP_SEPARATOR, relationshipsAST.get(2).tag());
    assertEquals(StructuralTag.RECORD, relationshipsAST.get(3).tag());
    assertEquals(Person.class, relationshipsAST.get(3).type());

    assertEquals("MAP<STRING, RECORD>", relationshipsAST.toStructureString());
  }

  // ===============================
  // Example 5: Complex Animal Hierarchy
  // ===============================

  public sealed interface Animal permits Mammal, Bird, Alicorn {}
  public sealed interface Mammal extends Animal permits Dog, Cat {}
  public sealed interface Bird extends Animal permits Eagle, Penguin {}
  public record Alicorn(String name, String[] magicPowers) implements Animal {}
  public record Dog(String name, int age) implements Mammal {}
  public record Cat(String name, boolean purrs) implements Mammal {}
  public record Eagle(double wingspan) implements Bird {}
  public record Penguin(boolean canSwim) implements Bird {}

  @Test
  @DisplayName("README Example: Animal Sealed Hierarchy Analysis")
  void testAnimalSealedHierarchy() {
    Map<Class<?>, TypeStructureAST> hierarchy = MetaStage.analyzeSealedHierarchy(Animal.class);

    // Should contain all concrete implementations plus intermediate sealed interfaces
    assertTrue(hierarchy.containsKey(Alicorn.class));
    assertTrue(hierarchy.containsKey(Dog.class));
    assertTrue(hierarchy.containsKey(Cat.class));
    assertTrue(hierarchy.containsKey(Eagle.class));
    assertTrue(hierarchy.containsKey(Penguin.class));
    assertTrue(hierarchy.containsKey(Mammal.class));
    assertTrue(hierarchy.containsKey(Bird.class));

    // Test specific record structures
    TypeStructureAST alicornAST = hierarchy.get(Alicorn.class);
    assertEquals(StructuralTag.RECORD, alicornAST.root().tag());

    TypeStructureAST mammalAST = hierarchy.get(Mammal.class);
    assertEquals(StructuralTag.INTERFACE, mammalAST.root().tag());
  }

  @Test
  @DisplayName("README Example: Alicorn Record with String Array")
  void testAlicornRecordStructure() {
    Map<String, TypeStructureAST> alicornComponents = MetaStage.analyzeRecordComponents(Alicorn.class);

    assertEquals(2, alicornComponents.size());
    assertTrue(alicornComponents.containsKey("name"));
    assertTrue(alicornComponents.containsKey("magicPowers"));

    // name: String
    TypeStructureAST nameAST = alicornComponents.get("name");
    assertEquals(1, nameAST.size());
    assertEquals(StructuralTag.STRING, nameAST.root().tag());

    // magicPowers: String[]
    TypeStructureAST powersAST = alicornComponents.get("magicPowers");
    assertEquals(2, powersAST.size());
    assertEquals(StructuralTag.ARRAY, powersAST.get(0).tag());
    assertEquals(StructuralTag.STRING, powersAST.get(1).tag());
    assertEquals("ARRAY<STRING>", powersAST.toStructureString());
  }

  @Test
  @DisplayName("README Example: Dog and Cat Record Structures")
  void testMammalRecordStructures() {
    // Test Dog record
    Map<String, TypeStructureAST> dogComponents = MetaStage.analyzeRecordComponents(Dog.class);
    assertEquals(2, dogComponents.size());

    TypeStructureAST dogNameAST = dogComponents.get("name");
    assertEquals(StructuralTag.STRING, dogNameAST.root().tag());

    TypeStructureAST dogAgeAST = dogComponents.get("age");
    assertEquals(StructuralTag.INTEGER, dogAgeAST.root().tag());

    // Test Cat record
    Map<String, TypeStructureAST> catComponents = MetaStage.analyzeRecordComponents(Cat.class);
    assertEquals(2, catComponents.size());

    TypeStructureAST catNameAST = catComponents.get("name");
    assertEquals(StructuralTag.STRING, catNameAST.root().tag());

    TypeStructureAST catPurrsAST = catComponents.get("purrs");
    assertEquals(StructuralTag.BOOLEAN, catPurrsAST.root().tag());
  }

  @Test
  @DisplayName("README Example: Eagle and Penguin Record Structures")
  void testBirdRecordStructures() {
    // Test Eagle record
    Map<String, TypeStructureAST> eagleComponents = MetaStage.analyzeRecordComponents(Eagle.class);
    assertEquals(1, eagleComponents.size());

    TypeStructureAST wingspanAST = eagleComponents.get("wingspan");
    assertEquals(StructuralTag.DOUBLE, wingspanAST.root().tag());

    // Test Penguin record
    Map<String, TypeStructureAST> penguinComponents = MetaStage.analyzeRecordComponents(Penguin.class);
    assertEquals(1, penguinComponents.size());

    TypeStructureAST canSwimAST = penguinComponents.get("canSwim");
    assertEquals(StructuralTag.BOOLEAN, canSwimAST.root().tag());
  }

  // ===============================
  // Example 6: Complex Type from Academic Analysis
  // List<Map<String, Optional<Integer[]>[]>>
  // ===============================

  @Test
  @DisplayName("README Academic Example: List<Map<String, Optional<Integer[]>[]>>")
  void testComplexAcademicExample() throws Exception {
    Type complexType = getClass().getDeclaredField("complexAcademicExample").getGenericType();
    TypeStructureAST ast = MetaStage.analyze(complexType);

    // Expected structure from academic analysis:
    // LIST, MAP, STRING, MAP_SEPARATOR, ARRAY, OPTIONAL, ARRAY, INTEGER
    List<StructuralTag> expectedTags = List.of(
        StructuralTag.LIST,
        StructuralTag.MAP,
        StructuralTag.STRING,
        StructuralTag.MAP_SEPARATOR,
        StructuralTag.ARRAY,
        StructuralTag.OPTIONAL,
        StructuralTag.ARRAY,
        StructuralTag.INTEGER
    );

    assertEquals(expectedTags.size(), ast.size());
    for (int i = 0; i < expectedTags.size(); i++) {
      assertEquals(expectedTags.get(i), ast.get(i).tag(),
          "Position " + i + ": expected " + expectedTags.get(i) + " but got " + ast.get(i).tag());
    }

    // Verify the structure string representation
    String expectedStructure = "LIST<MAP<STRING, ARRAY<OPTIONAL<ARRAY<INTEGER>>>>>";
    assertEquals(expectedStructure, ast.toStructureString());

    // Verify nested properties
    assertTrue(ast.isNested());
    assertFalse(ast.isSimple());
    // Actually: LIST(1) + MAP(1) + ARRAY(1) + OPTIONAL(1) + ARRAY(1) = 5 containers
    assertEquals(5, ast.containerCount());
  }

  // ===============================
  // Example 7: Wire Protocol Supported Types
  // ===============================

  @Test
  @DisplayName("README Wire Protocol: All Primitive Types")
  void testWireProtocolPrimitiveTypes() {
    // Test all primitive types mentioned in wire protocol table
    Class<?>[] primitiveTypes = {
        boolean.class, Boolean.class,
        byte.class, Byte.class,
        short.class, Short.class,
        char.class, Character.class,
        int.class, Integer.class,
        long.class, Long.class,
        float.class, Float.class,
        double.class, Double.class,
        String.class,
        UUID.class
    };

    for (Class<?> primitiveType : primitiveTypes) {
      TypeStructureAST ast = MetaStage.analyze(primitiveType);
      assertEquals(1, ast.size());
      assertTrue(ast.isSimple());
      assertTrue(ast.root().isPrimitive());
    }
  }

  @Test
  @DisplayName("README Wire Protocol: Container Types")
  void testWireProtocolContainerTypes() throws Exception {
    // Array
    TypeStructureAST arrayAST = MetaStage.analyze(int[].class);
    assertEquals(StructuralTag.ARRAY, arrayAST.root().tag());

    // List
    Type listType = getClass().getDeclaredField("stringList").getGenericType();
    TypeStructureAST listAST = MetaStage.analyze(listType);
    assertEquals(StructuralTag.LIST, listAST.root().tag());

    // Optional
    Type optionalType = getClass().getDeclaredField("optionalString").getGenericType();
    TypeStructureAST optionalAST = MetaStage.analyze(optionalType);
    assertEquals(StructuralTag.OPTIONAL, optionalAST.root().tag());

    // Map
    Type mapType = getClass().getDeclaredField("stringIntMap").getGenericType();
    TypeStructureAST mapAST = MetaStage.analyze(mapType);
    assertEquals(StructuralTag.MAP, mapAST.root().tag());
  }

  @Test
  @DisplayName("README Validation: All Examples Produce Valid ASTs")
  void testAllExamplesProduceValidASTs() {
    // Collect all ASTs from examples
    List<TypeStructureAST> allASTs = new ArrayList<>();

    // Add primitive examples
    allASTs.add(MetaStage.analyze(String.class));
    allASTs.add(MetaStage.analyze(int.class));
    allASTs.add(MetaStage.analyze(Season.class));

    // Add record examples
    allASTs.addAll(MetaStage.analyzeRecordComponents(Month.class).values());
    allASTs.addAll(MetaStage.analyzeRecordComponents(InternalNode.class).values());
    allASTs.addAll(MetaStage.analyzeRecordComponents(Alicorn.class).values());

    // Add sealed interface examples
    allASTs.addAll(MetaStage.analyzeSealedHierarchy(TreeNode.class).values());
    allASTs.addAll(MetaStage.analyzeSealedHierarchy(Animal.class).values());

    // Validate all ASTs
    for (TypeStructureAST ast : allASTs) {
      assertDoesNotThrow(() -> MetaStage.validateSerializationSupport(ast),
          "AST should be valid for serialization: " + ast.toStructureString());
    }
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
