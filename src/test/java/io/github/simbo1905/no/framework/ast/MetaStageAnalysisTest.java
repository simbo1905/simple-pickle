package io.github.simbo1905.no.framework.ast;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.lang.reflect.Type;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Comprehensive test suite for AST construction with progressive complexity increases.
 *
 * This test suite validates the Abstract Syntax Tree construction for Java's generic
 * type system, covering the complete range from simple primitives to complex nested
 * sealed interface hierarchies as specified in the formal EBNF grammar.
 *
 * Test Structure:
 * 1. Primitive Types (Boolean through UUID)
 * 2. Simple Containers (Array, List, Optional)
 * 3. Map Structures (with key/value separation)
 * 4. User-Defined Types (Records, Enums, Interfaces)
 * 5. Complex Nested Structures
 * 6. README.md Examples
 * 7. Edge Cases and Error Conditions
 */
class MetaStageAnalysisTest {

  @BeforeEach
  void setUp() {
    // Clear cache before each test to ensure isolation
    MetaStage.clearCache();
  }

  @AfterEach
  void tearDown() {
    // Clear cache after each test to prevent memory leaks
    MetaStage.clearCache();
  }

  // Test Records and Enums for progressive complexity testing
  public record SimpleRecord(int value) {}
  public record ComplexRecord(String name, List<Integer> numbers, Optional<Double> optionalValue) {}
  public record NestedRecord(SimpleRecord inner, String[] tags) {}
  public record MapRecord(Map<String, Integer> stringToInt, Map<UUID, List<String>> complexMap) {}

  public enum SimpleEnum { ONE, TWO, THREE }
  public enum ComplexEnum {
    ALPHA("a"), BETA("b"), GAMMA("g");
    final String code;
    ComplexEnum(String code) { this.code = code; }
    public String getCode() { return code; }
  }

  // Sealed interface hierarchy from README.md
  public sealed interface TreeNode permits InternalNode, LeafNode, TreeEnum {}
  public record InternalNode(String name, TreeNode left, TreeNode right) implements TreeNode {}
  public record LeafNode(int value) implements TreeNode {}
  public enum TreeEnum implements TreeNode { EMPTY }

  // Nested sealed interface hierarchy from README.md
  public sealed interface Animal permits Mammal, Bird, Alicorn {}
  public sealed interface Mammal extends Animal permits Dog, Cat {}
  public sealed interface Bird extends Animal permits Eagle, Penguin {}
  public record Alicorn(String name, String[] magicPowers) implements Animal {}
  public record Dog(String name, int age) implements Mammal {}
  public record Cat(String name, boolean purrs) implements Mammal {}
  public record Eagle(double wingspan) implements Bird {}
  public record Penguin(boolean canSwim) implements Bird {}

  /**
   * Test Category 1: Primitive Types
   * Validates AST construction for all supported primitive types.
   */
  @Nested
  @DisplayName("Primitive Type Analysis")
  class PrimitiveTypeTests {

    @ParameterizedTest
    @ValueSource(classes = {
        boolean.class, Boolean.class,
        byte.class, Byte.class,
        short.class, Short.class,
        char.class, Character.class,
        int.class, Integer.class,
        long.class, Long.class,
        float.class, Float.class,
        double.class, Double.class
    })
    @DisplayName("Primitive and Boxed Types")
    void testPrimitiveTypes(Class<?> primitiveClass) {
      TypeStructureAST ast = MetaStage.analyze(primitiveClass);

      assertNotNull(ast);
      assertEquals(1, ast.size(), "Primitive type should have single tag");
      assertTrue(ast.isSimple(), "Primitive type should be simple");
      assertFalse(ast.isNested(), "Primitive type should not be nested");

      TagWithType tag = ast.root();
      assertTrue(tag.isPrimitive(), "Should be classified as primitive");
      assertEquals(primitiveClass, tag.type(), "Type should match input");
    }

    @Test
    @DisplayName("String Type")
    void testStringType() {
      TypeStructureAST ast = MetaStage.analyze(String.class);

      assertEquals(1, ast.size());
      assertTrue(ast.isSimple());

      TagWithType tag = ast.root();
      assertEquals(StructuralTag.STRING, tag.tag());
      assertEquals(String.class, tag.type());
      assertTrue(tag.isPrimitive());
    }

    @Test
    @DisplayName("UUID Type")
    void testUUIDType() {
      TypeStructureAST ast = MetaStage.analyze(UUID.class);

      assertEquals(1, ast.size());
      assertTrue(ast.isSimple());

      TagWithType tag = ast.root();
      assertEquals(StructuralTag.UUID, tag.tag());
      assertEquals(UUID.class, tag.type());
      assertTrue(tag.isPrimitive());
    }
  }

  /**
   * Test Category 2: Simple Container Types
   * Validates AST construction for single-level container types.
   */
  @Nested
  @DisplayName("Simple Container Analysis")
  class SimpleContainerTests {

    @Test
    @DisplayName("Primitive Array")
    void testPrimitiveArray() {
      TypeStructureAST ast = MetaStage.analyze(int[].class);

      assertEquals(2, ast.size());
      assertTrue(ast.isNested());
      assertFalse(ast.isSimple());
      assertEquals(1, ast.containerCount());

      // Verify structure: ARRAY -> INTEGER
      TagWithType arrayTag = ast.get(0);
      assertEquals(StructuralTag.ARRAY, arrayTag.tag());
      assertTrue(arrayTag.isContainer());

      TagWithType intTag = ast.get(1);
      assertEquals(StructuralTag.INTEGER, intTag.tag());
      assertTrue(intTag.isPrimitive());

      assertEquals("ARRAY<INTEGER>", ast.toStructureString());
    }

    @Test
    @DisplayName("Object Array")
    void testObjectArray() {
      TypeStructureAST ast = MetaStage.analyze(String[].class);

      assertEquals(2, ast.size());

      TagWithType arrayTag = ast.get(0);
      assertEquals(StructuralTag.ARRAY, arrayTag.tag());

      TagWithType stringTag = ast.get(1);
      assertEquals(StructuralTag.STRING, stringTag.tag());

      assertEquals("ARRAY<STRING>", ast.toStructureString());
    }

    @Test
    @DisplayName("List of Integers")
    void testListOfIntegers() throws Exception {
      // Use reflection to get List<Integer> type
      Type listType = getClass().getDeclaredField("integerList").getGenericType();
      TypeStructureAST ast = MetaStage.analyze(listType);

      assertEquals(2, ast.size());
      assertTrue(ast.isNested());

      TagWithType listTag = ast.get(0);
      assertEquals(StructuralTag.LIST, listTag.tag());
      assertTrue(listTag.isContainer());

      TagWithType intTag = ast.get(1);
      assertEquals(StructuralTag.INTEGER, intTag.tag());

      assertEquals("LIST<INTEGER>", ast.toStructureString());
    }

    @Test
    @DisplayName("Optional String")
    void testOptionalString() throws Exception {
      Type optionalType = getClass().getDeclaredField("optionalString").getGenericType();
      TypeStructureAST ast = MetaStage.analyze(optionalType);

      assertEquals(2, ast.size());

      TagWithType optionalTag = ast.get(0);
      assertEquals(StructuralTag.OPTIONAL, optionalTag.tag());
      assertTrue(optionalTag.isContainer());

      TagWithType stringTag = ast.get(1);
      assertEquals(StructuralTag.STRING, stringTag.tag());

      assertEquals("OPTIONAL<STRING>", ast.toStructureString());
    }

    // Fields needed for reflection-based generic type access
    @SuppressWarnings("unused")
    List<Integer> integerList;
    @SuppressWarnings("unused")
    Optional<String> optionalString;
  }

  /**
   * Test Category 3: Map Structure Analysis
   * Validates special handling of Map types with key/value separation.
   */
  @Nested
  @DisplayName("Map Structure Analysis")
  class MapStructureTests {

    @Test
    @DisplayName("Simple Map String to Integer")
    void testSimpleMap() throws Exception {
      Type mapType = getClass().getDeclaredField("stringIntMap").getGenericType();
      TypeStructureAST ast = MetaStage.analyze(mapType);

      assertEquals(4, ast.size()); // MAP, STRING, MAP_SEPARATOR, INTEGER
      assertTrue(ast.isNested());

      assertEquals(StructuralTag.MAP, ast.get(0).tag());
      assertEquals(StructuralTag.STRING, ast.get(1).tag());
      assertEquals(StructuralTag.MAP_SEPARATOR, ast.get(2).tag());
      assertEquals(StructuralTag.INTEGER, ast.get(3).tag());

      assertEquals("MAP<STRING, INTEGER>", ast.toStructureString());
    }

    @Test
    @DisplayName("Complex Map with List Values")
    void testComplexMap() throws Exception {
      Type mapType = getClass().getDeclaredField("complexMap").getGenericType();
      TypeStructureAST ast = MetaStage.analyze(mapType);

      // MAP, UUID, MAP_SEPARATOR, LIST, STRING
      assertEquals(5, ast.size());

      assertEquals(StructuralTag.MAP, ast.get(0).tag());
      assertEquals(StructuralTag.UUID, ast.get(1).tag());
      assertEquals(StructuralTag.MAP_SEPARATOR, ast.get(2).tag());
      assertEquals(StructuralTag.LIST, ast.get(3).tag());
      assertEquals(StructuralTag.STRING, ast.get(4).tag());

      assertEquals("MAP<UUID, LIST<STRING>>", ast.toStructureString());
    }

    @SuppressWarnings("unused")
    Map<String, Integer> stringIntMap;
    @SuppressWarnings("unused")
    Map<UUID, List<String>> complexMap;
  }

  /**
   * Test Category 4: User-Defined Types
   * Validates AST construction for records, enums, and sealed interfaces.
   */
  @Nested
  @DisplayName("User-Defined Type Analysis")
  class UserDefinedTypeTests {

    @Test
    @DisplayName("Simple Record")
    void testSimpleRecord() {
      TypeStructureAST ast = MetaStage.analyze(SimpleRecord.class);

      assertEquals(1, ast.size());
      assertTrue(ast.isSimple());

      TagWithType tag = ast.root();
      assertEquals(StructuralTag.RECORD, tag.tag());
      assertEquals(SimpleRecord.class, tag.type());
      assertTrue(tag.isUserDefined());
    }

    @Test
    @DisplayName("Simple Enum")
    void testSimpleEnum() {
      TypeStructureAST ast = MetaStage.analyze(SimpleEnum.class);

      assertEquals(1, ast.size());
      assertTrue(ast.isSimple());

      TagWithType tag = ast.root();
      assertEquals(StructuralTag.ENUM, tag.tag());
      assertEquals(SimpleEnum.class, tag.type());
      assertTrue(tag.isUserDefined());
    }

    @Test
    @DisplayName("Sealed Interface")
    void testSealedInterface() {
      TypeStructureAST ast = MetaStage.analyze(TreeNode.class);

      assertEquals(1, ast.size());
      assertTrue(ast.isSimple());

      TagWithType tag = ast.root();
      assertEquals(StructuralTag.INTERFACE, tag.tag());
      assertEquals(TreeNode.class, tag.type());
      assertTrue(tag.isUserDefined());
    }

    @Test
    @DisplayName("Record Component Analysis")
    void testRecordComponentAnalysis() {
      Map<String, TypeStructureAST> components = MetaStage.analyzeRecordComponents(ComplexRecord.class);

      assertEquals(3, components.size());
      assertTrue(components.containsKey("name"));
      assertTrue(components.containsKey("numbers"));
      assertTrue(components.containsKey("optionalValue"));

      // Verify name component (String)
      TypeStructureAST nameAST = components.get("name");
      assertEquals(1, nameAST.size());
      assertEquals(StructuralTag.STRING, nameAST.root().tag());

      // Verify numbers component (List<Integer>)
      TypeStructureAST numbersAST = components.get("numbers");
      assertEquals(2, numbersAST.size());
      assertEquals(StructuralTag.LIST, numbersAST.get(0).tag());
      assertEquals(StructuralTag.INTEGER, numbersAST.get(1).tag());

      // Verify optionalValue component (Optional<Double>)
      TypeStructureAST optionalAST = components.get("optionalValue");
      assertEquals(2, optionalAST.size());
      assertEquals(StructuralTag.OPTIONAL, optionalAST.get(0).tag());
      assertEquals(StructuralTag.DOUBLE, optionalAST.get(1).tag());
    }

    @Test
    @DisplayName("Sealed Hierarchy Analysis")
    void testSealedHierarchyAnalysis() {
      Map<Class<?>, TypeStructureAST> hierarchy = MetaStage.analyzeSealedHierarchy(TreeNode.class);

      assertEquals(3, hierarchy.size());
      assertTrue(hierarchy.containsKey(InternalNode.class));
      assertTrue(hierarchy.containsKey(LeafNode.class));
      assertTrue(hierarchy.containsKey(TreeEnum.class));

      // Verify each permitted type
      TypeStructureAST internalAST = hierarchy.get(InternalNode.class);
      assertEquals(StructuralTag.RECORD, internalAST.root().tag());

      TypeStructureAST leafAST = hierarchy.get(LeafNode.class);
      assertEquals(StructuralTag.RECORD, leafAST.root().tag());

      TypeStructureAST enumAST = hierarchy.get(TreeEnum.class);
      assertEquals(StructuralTag.ENUM, enumAST.root().tag());
    }

    @Test
    @DisplayName("Nested Sealed Hierarchy Analysis")
    void testNestedSealedHierarchyAnalysis() {
      Map<Class<?>, TypeStructureAST> hierarchy = MetaStage.analyzeSealedHierarchy(Animal.class);

      // Should include all classes from the nested hierarchy
      assertTrue(hierarchy.containsKey(Alicorn.class));
      assertTrue(hierarchy.containsKey(Dog.class));
      assertTrue(hierarchy.containsKey(Cat.class));
      assertTrue(hierarchy.containsKey(Eagle.class));
      assertTrue(hierarchy.containsKey(Penguin.class));

      // Should also include the intermediate sealed interfaces
      assertTrue(hierarchy.containsKey(Mammal.class));
      assertTrue(hierarchy.containsKey(Bird.class));
    }
  }

  /**
   * Test Category 5: Complex Nested Structures
   * Validates AST construction for deeply nested and complex type structures.
   */
  @Nested
  @DisplayName("Complex Nested Structure Analysis")
  class ComplexNestedTests {

    @Test
    @DisplayName("List of Optional String Arrays")
    void testListOptionalStringArray() throws Exception {
      Type complexType = getClass().getDeclaredField("listOptionalStringArray").getGenericType();
      TypeStructureAST ast = MetaStage.analyze(complexType);

      // LIST, OPTIONAL, ARRAY, STRING
      assertEquals(4, ast.size());
      assertTrue(ast.isNested());
      assertEquals(3, ast.containerCount());

      assertEquals(StructuralTag.LIST, ast.get(0).tag());
      assertEquals(StructuralTag.OPTIONAL, ast.get(1).tag());
      assertEquals(StructuralTag.ARRAY, ast.get(2).tag());
      assertEquals(StructuralTag.STRING, ast.get(3).tag());

      assertEquals("LIST<OPTIONAL<ARRAY<STRING>>>", ast.toStructureString());
    }

    @Test
    @DisplayName("Map with Complex Nested Values")
    void testMapComplexNested() throws Exception {
      Type complexType = getClass().getDeclaredField("mapComplexNested").getGenericType();
      TypeStructureAST ast = MetaStage.analyze(complexType);

      // MAP, STRING, MAP_SEPARATOR, OPTIONAL, ARRAY, INTEGER
      assertEquals(6, ast.size());

      assertEquals(StructuralTag.MAP, ast.get(0).tag());
      assertEquals(StructuralTag.STRING, ast.get(1).tag());
      assertEquals(StructuralTag.MAP_SEPARATOR, ast.get(2).tag());
      assertEquals(StructuralTag.OPTIONAL, ast.get(3).tag());
      assertEquals(StructuralTag.ARRAY, ast.get(4).tag());
      assertEquals(StructuralTag.INTEGER, ast.get(5).tag());

      assertEquals("MAP<STRING, OPTIONAL<ARRAY<INTEGER>>>", ast.toStructureString());
    }

    @Test
    @DisplayName("Deeply Nested Record Structure")
    void testDeeplyNestedRecord() {
      Map<String, TypeStructureAST> components = MetaStage.analyzeRecordComponents(NestedRecord.class);

      assertEquals(2, components.size());

      // inner component should be SimpleRecord
      TypeStructureAST innerAST = components.get("inner");
      assertEquals(1, innerAST.size());
      assertEquals(StructuralTag.RECORD, innerAST.root().tag());
      assertEquals(SimpleRecord.class, innerAST.root().type());

      // tags component should be String[]
      TypeStructureAST tagsAST = components.get("tags");
      assertEquals(2, tagsAST.size());
      assertEquals(StructuralTag.ARRAY, tagsAST.get(0).tag());
      assertEquals(StructuralTag.STRING, tagsAST.get(1).tag());
    }

    @SuppressWarnings("unused")
    List<Optional<String[]>> listOptionalStringArray;
    @SuppressWarnings("unused")
    Map<String, Optional<Integer[]>> mapComplexNested;
  }

  /**
   * Test Category 6: README.md Examples
   * Validates specific examples mentioned in the README.md documentation.
   */
  @Nested
  @DisplayName("README.md Example Validation")
  class ReadmeExampleTests {

    @Test
    @DisplayName("TreeNode Sealed Interface Example")
    void testTreeNodeExample() {
      // Test the sealed interface itself
      TypeStructureAST treeNodeAST = MetaStage.analyze(TreeNode.class);
      assertEquals(StructuralTag.INTERFACE, treeNodeAST.root().tag());

      // Test InternalNode record components
      Map<String, TypeStructureAST> internalComponents = MetaStage.analyzeRecordComponents(InternalNode.class);
      assertEquals(3, internalComponents.size());

      // name: String
      TypeStructureAST nameAST = internalComponents.get("name");
      assertEquals(StructuralTag.STRING, nameAST.root().tag());

      // left: TreeNode (interface)
      TypeStructureAST leftAST = internalComponents.get("left");
      assertEquals(StructuralTag.INTERFACE, leftAST.root().tag());
      assertEquals(TreeNode.class, leftAST.root().type());

      // right: TreeNode (interface)
      TypeStructureAST rightAST = internalComponents.get("right");
      assertEquals(StructuralTag.INTERFACE, rightAST.root().tag());
      assertEquals(TreeNode.class, rightAST.root().type());
    }

    @Test
    @DisplayName("Animal Hierarchy Example")
    void testAnimalHierarchyExample() {
      // Test Alicorn record with string array
      Map<String, TypeStructureAST> alicornComponents = MetaStage.analyzeRecordComponents(Alicorn.class);

      // name: String
      TypeStructureAST nameAST = alicornComponents.get("name");
      assertEquals(StructuralTag.STRING, nameAST.root().tag());

      // magicPowers: String[]
      TypeStructureAST powersAST = alicornComponents.get("magicPowers");
      assertEquals(2, powersAST.size());
      assertEquals(StructuralTag.ARRAY, powersAST.get(0).tag());
      assertEquals(StructuralTag.STRING, powersAST.get(1).tag());
      assertEquals("ARRAY<STRING>", powersAST.toStructureString());
    }

    @Test
    @DisplayName("Complex Map Examples from README")
    void testReadmeMapExamples() {
      // Test MapRecord with multiple map types
      Map<String, TypeStructureAST> mapComponents = MetaStage.analyzeRecordComponents(MapRecord.class);

      // stringToInt: Map<String, Integer>
      TypeStructureAST simpleMapAST = mapComponents.get("stringToInt");
      assertEquals(4, simpleMapAST.size()); // MAP, STRING, MAP_SEPARATOR, INTEGER
      assertEquals("MAP<STRING, INTEGER>", simpleMapAST.toStructureString());

      // complexMap: Map<UUID, List<String>>
      TypeStructureAST complexMapAST = mapComponents.get("complexMap");
      assertEquals(5, complexMapAST.size()); // MAP, UUID, MAP_SEPARATOR, LIST, STRING
      assertEquals("MAP<UUID, LIST<STRING>>", complexMapAST.toStructureString());
    }

    @Test
    @DisplayName("List<Map<String, Optional<Integer[]>[]>> Complex Example")
    void testComplexReadmeExample() throws Exception {
      // This matches the complex example mentioned in the academic analysis
      Type complexType = getClass().getDeclaredField("readmeComplexExample").getGenericType();
      TypeStructureAST ast = MetaStage.analyze(complexType);

      // Expected: LIST, MAP, STRING, MAP_SEPARATOR, ARRAY, OPTIONAL, ARRAY, INTEGER
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
            "Mismatch at position " + i + ": expected " + expectedTags.get(i) + " but got " + ast.get(i).tag());
      }

      assertEquals("LIST<MAP<STRING, ARRAY<OPTIONAL<ARRAY<INTEGER>>>>>", ast.toStructureString());
    }

    @SuppressWarnings("unused")
    List<Map<String, Optional<Integer[]>[]>> readmeComplexExample;
  }

  /**
   * Test Category 7: Edge Cases and Error Conditions
   * Validates proper error handling and edge case behavior.
   */
  @Nested
  @DisplayName("Edge Cases and Error Handling")
  class EdgeCaseTests {

    @Test
    @DisplayName("Null Type Analysis")
    void testNullTypeAnalysis() {
      assertThrows(NullPointerException.class, () -> MetaStage.analyze(null));
    }

    @Test
    @DisplayName("Non-Record Class for Component Analysis")
    void testNonRecordComponentAnalysis() {
      assertThrows(IllegalArgumentException.class,
          () -> MetaStage.analyzeRecordComponents(String.class));
    }

    @Test
    @DisplayName("Non-Sealed Interface for Hierarchy Analysis")
    void testNonSealedHierarchyAnalysis() {
      assertThrows(IllegalArgumentException.class,
          () -> MetaStage.analyzeSealedHierarchy(List.class));
    }

    @Test
    @DisplayName("Cache Functionality")
    void testCacheFunctionality() {
      assertEquals(0, MetaStage.getCacheSize());

      TypeStructureAST ast1 = MetaStage.analyze(String.class);
      assertEquals(1, MetaStage.getCacheSize());

      TypeStructureAST ast2 = MetaStage.analyze(String.class);
      assertEquals(1, MetaStage.getCacheSize()); // Should not increase

      assertSame(ast1, ast2, "Should return cached instance");

      MetaStage.clearCache();
      assertEquals(0, MetaStage.getCacheSize());
    }

    @Test
    @DisplayName("AST Validation - Empty AST")
    void testValidationEmptyAST() {
      TypeStructureAST emptyAST = new TypeStructureAST(List.of());
      assertThrows(IllegalArgumentException.class,
          () -> MetaStage.validateSerializationSupport(emptyAST));
    }

    @Test
    @DisplayName("AST Validation - Null AST")
    void testValidationNullAST() {
      assertThrows(NullPointerException.class,
          () -> MetaStage.validateSerializationSupport(null));
    }

    @Test
    @DisplayName("AST Substructure Operations")
    void testASTSubstructureOperations() {
      TypeStructureAST ast = MetaStage.analyze(int[].class);

      // Test substructure
      TypeStructureAST sub = ast.substructure(1);
      assertEquals(1, sub.size());
      assertEquals(StructuralTag.INTEGER, sub.root().tag());

      // Test bounds checking
      assertThrows(IndexOutOfBoundsException.class, () -> ast.substructure(-1));
      assertThrows(IndexOutOfBoundsException.class, () -> ast.substructure(ast.size()));
    }

    @Test
    @DisplayName("AST Structure String Representation")
    void testASTStringRepresentation() {
      TypeStructureAST emptyAST = new TypeStructureAST(List.of());
      assertEquals("<empty>", emptyAST.toStructureString());

      TypeStructureAST simpleAST = MetaStage.analyze(String.class);
      assertEquals("STRING", simpleAST.toStructureString());

      TypeStructureAST arrayAST = MetaStage.analyze(int[].class);
      assertEquals("ARRAY<INTEGER>", arrayAST.toStructureString());
    }
  }

  /**
   * Test Category 8: Performance and Scalability
   * Validates performance characteristics and scalability of the analysis.
   */
  @Nested
  @DisplayName("Performance and Scalability")
  class PerformanceTests {

    @Test
    @DisplayName("Deep Nesting Performance")
    void testDeepNestingPerformance() throws Exception {
      // Test deeply nested structure for performance
      Type deepType = getClass().getDeclaredField("deeplyNested").getGenericType();

      long startTime = System.nanoTime();
      TypeStructureAST ast = MetaStage.analyze(deepType);
      long endTime = System.nanoTime();

      assertNotNull(ast);
      assertTrue(ast.size() > 5, "Should have multiple levels of nesting");

      // Analysis should complete within reasonable time (less than 10ms)
      long durationMs = (endTime - startTime) / 1_000_000;
      assertTrue(durationMs < 10, "Analysis took too long: " + durationMs + "ms");
    }

    @Test
    @DisplayName("Cache Performance Impact")
    void testCachePerformance() {
      // First analysis (cache miss)
      long startTime1 = System.nanoTime();
      TypeStructureAST ast1 = MetaStage.analyze(ComplexRecord.class);
      long endTime1 = System.nanoTime();

      // Second analysis (cache hit)
      long startTime2 = System.nanoTime();
      TypeStructureAST ast2 = MetaStage.analyze(ComplexRecord.class);
      long endTime2 = System.nanoTime();

      long duration1 = endTime1 - startTime1;
      long duration2 = endTime2 - startTime2;

      // Cache hit should be significantly faster
      assertTrue(duration2 < duration1 / 2,
          "Cache hit should be at least 2x faster than cache miss");

      assertSame(ast1, ast2, "Should return same cached instance");
    }

    @SuppressWarnings("unused")
    List<Optional<Map<String, List<Integer[]>>>> deeplyNested;
  }
}
