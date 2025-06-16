package io.github.simbo1905.no.framework.ast;
import static io.github.simbo1905.no.framework.Pickler.LOGGER;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Type;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Full integration test suite that validates the complete AST construction system
 * against the formal EBNF grammar and README.md requirements.
 *
 * This test suite serves as the comprehensive validation that the implementation
 * correctly handles all aspects of the multi-stage programming approach:
 *
 * 1. AST Construction: Validates recursive descent parsing
 * 2. Staged Metaprogramming: Validates compile-time analysis capabilities
 * 3. Static Type Analysis: Validates type safety and serialization support
 * 4. EBNF Grammar Compliance: Validates formal grammar conformance
 * 5. README.md Examples: Validates all documented use cases
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class FullIntegrationTest {

  // Complete type hierarchy for comprehensive testing
  public sealed interface Vehicle permits Car, Truck, Motorcycle {}
  public record Car(String make, String model, int year, Engine engine) implements Vehicle {}
  public record Truck(String make, int payloadCapacity, Engine engine) implements Vehicle {}
  public record Motorcycle(String make, int engineCC) implements Vehicle {}

  public record Engine(String type, double displacement, Optional<Turbocharger> turbo) {}
  public record Turbocharger(String manufacturer, double boost) {}

  public enum FuelType { GASOLINE, DIESEL, ELECTRIC, HYBRID }

  public record Fleet(
      String name,
      List<Vehicle> vehicles,
      Map<String, List<String>> maintenanceRecords,
      Optional<Map<UUID, FuelType[]>> fuelAssignments
  ) {}

  @BeforeAll
  static void setupIntegrationTest() {
    LOGGER.finer(() -> String.format("=== Starting Full Integration Test Suite (cache size: %d) ===", MetaStage.TYPE_CACHE.size()));
    MetaStage.TYPE_CACHE.clear();
    LOGGER.finer(() -> String.format("Cache cleared, new size: %d", MetaStage.TYPE_CACHE.size()));
  }

  @AfterAll
  static void teardownIntegrationTest() {
    LOGGER.finer(() -> "=== Integration Test Suite Complete ===");
    LOGGER.finer(() -> String.format("Final cache size: %d entries%n", MetaStage.TYPE_CACHE.size()));
    MetaStage.TYPE_CACHE.clear();
  }

  @Test
  @Order(1)
  @DisplayName("Integration Test 1: EBNF Grammar Compliance Validation")
  void testEBNFGrammarCompliance() {
    LOGGER.finer(() -> "\n--- Testing EBNF Grammar Compliance ---");

    // Test the formal grammar: TypeStructure ::= TagTypeSequence
    TypeStructureAST simpleAST = MetaStage.analyze(String.class);
    assertNotNull(simpleAST);
    assertFalse(simpleAST.isEmpty());

    // Test: TagTypeSequence ::= TagWithType { TagWithType }
    assertTrue(simpleAST.tagTypes().size() >= 1);

    // Test: TagWithType ::= Tag Type
    for (TagWithType tagType : simpleAST.tagTypes()) {
      assertNotNull(tagType.tag());
      assertNotNull(tagType.type());
    }

    // Test container grammar: ContainerTag ::= 'ARRAY' | 'LIST' | 'OPTIONAL' | 'MAP'
    TypeStructureAST arrayAST = MetaStage.analyze(int[].class);
    assertEquals(StructuralTag.ARRAY, arrayAST.root().tag());
    assertTrue(arrayAST.root().isContainer());

    LOGGER.finer(() -> "  ✓ EBNF grammar compliance validated");
  }

  @Test
  @Order(2)
  @DisplayName("Integration Test 2: Multi-Stage Programming Workflow")
  void testMultiStageProgrammingWorkflow() {
    LOGGER.finer(() -> "\n--- Testing Multi-Stage Programming Workflow ---");

    // Meta-stage: Analyze complex type structure
    long metaStageStart = System.nanoTime();
    Map<String, TypeStructureAST> fleetComponents = MetaStage.analyzeRecordComponents(Fleet.class);
    long metaStageEnd = System.nanoTime();

    // Validate meta-stage results
    assertEquals(4, fleetComponents.size());
    assertTrue(fleetComponents.containsKey("name"));
    assertTrue(fleetComponents.containsKey("vehicles"));
    assertTrue(fleetComponents.containsKey("maintenanceRecords"));
    assertTrue(fleetComponents.containsKey("fuelAssignments"));

    // Validate that ASTs enable code generation (object-stage preparation)
    for (Map.Entry<String, TypeStructureAST> entry : fleetComponents.entrySet()) {
      TypeStructureAST ast = entry.getValue();

      // Each AST should be valid for serialization (object-stage compatibility)
      assertDoesNotThrow(() -> MetaStage.validateSerializationSupport(ast));

      // Each AST should provide clear structure for code generation
      String structureString = ast.toStructureString();
      assertNotNull(structureString);
      assertFalse(structureString.isEmpty());
      assertNotEquals("<empty>", structureString);
    }

    LOGGER.finer(() -> String.format("  ✓ Meta-stage analysis completed in %,d ns%n", metaStageEnd - metaStageStart));
    LOGGER.finer(() -> "  ✓ All ASTs validated for object-stage code generation");
  }

  @Test
  @Order(3)
  @DisplayName("Integration Test 3: Static Type Analysis Completeness")
  void testStaticTypeAnalysisCompleteness() {
    LOGGER.finer(() -> "\n--- Testing Static Type Analysis Completeness ---");

    // Test comprehensive type analysis
    Map<String, TypeStructureAST> engineComponents = MetaStage.analyzeRecordComponents(Engine.class);

    // type: String (primitive)
    TypeStructureAST typeAST = engineComponents.get("type");
    assertEquals(StructuralTag.STRING, typeAST.root().tag());
    assertTrue(typeAST.root().isPrimitive());

    // displacement: double (primitive)
    TypeStructureAST displacementAST = engineComponents.get("displacement");
    assertEquals(StructuralTag.DOUBLE, displacementAST.root().tag());
    assertTrue(displacementAST.root().isPrimitive());

    // turbo: Optional<Turbocharger> (container with user-defined type)
    TypeStructureAST turboAST = engineComponents.get("turbo");
    assertEquals(2, turboAST.size());
    assertEquals(StructuralTag.OPTIONAL, turboAST.get(0).tag());
    assertTrue(turboAST.get(0).isContainer());
    assertEquals(StructuralTag.RECORD, turboAST.get(1).tag());
    assertTrue(turboAST.get(1).isUserDefined());
    assertEquals(Turbocharger.class, turboAST.get(1).type());

    // Test nested record analysis
    Map<String, TypeStructureAST> turboComponents = MetaStage.analyzeRecordComponents(Turbocharger.class);
    assertEquals(2, turboComponents.size());

    LOGGER.finer(() -> "  ✓ Static type analysis handles all type categories");
    LOGGER.finer(() -> "  ✓ Primitive, container, and user-defined types correctly classified");
    LOGGER.finer(() -> "  ✓ Nested record analysis working correctly");
  }

  @Test
  @Order(4)
  @DisplayName("Integration Test 4: Complex Nested Structure Handling")
  void testComplexNestedStructureHandling() throws Exception {
    LOGGER.finer(() -> "\n--- Testing Complex Nested Structure Handling ---");

    // Test the most complex field: Optional<Map<UUID, FuelType[]>>
    Type complexType = Fleet.class.getDeclaredField("fuelAssignments").getGenericType();
    TypeStructureAST complexAST = MetaStage.analyze(complexType);

    // Expected structure: OPTIONAL, MAP, UUID, MAP_SEPARATOR, ARRAY, ENUM
    List<StructuralTag> expectedTags = List.of(
        StructuralTag.OPTIONAL,
        StructuralTag.MAP,
        StructuralTag.UUID,
        StructuralTag.MAP_SEPARATOR,
        StructuralTag.ARRAY,
        StructuralTag.ENUM
    );

    assertEquals(expectedTags.size(), complexAST.size());
    for (int i = 0; i < expectedTags.size(); i++) {
      assertEquals(expectedTags.get(i), complexAST.get(i).tag(),
          "Mismatch at position " + i);
    }

    // Validate structure properties
    assertTrue(complexAST.isNested());
    assertFalse(complexAST.isSimple());
    assertEquals(3, complexAST.containerCount()); // OPTIONAL, MAP, ARRAY

    // Validate map separator handling
    boolean foundSeparator = complexAST.tagTypes().stream()
        .anyMatch(tag -> tag.tag() == StructuralTag.MAP_SEPARATOR);
    assertTrue(foundSeparator, "Map structure should contain separator");

    LOGGER.finer(() -> String.format("  ✓ Complex structure analyzed: %s%n", complexAST.toStructureString()));
    LOGGER.finer(() -> "  ✓ Map separator correctly positioned");
    LOGGER.finer(() -> "  ✓ Container counting accurate");
  }

  @Test
  @Order(5)
  @DisplayName("Integration Test 5: Sealed Interface Hierarchy Analysis")
  void testSealedInterfaceHierarchyAnalysis() {
    LOGGER.finer(() -> "\n--- Testing Sealed Interface Hierarchy Analysis ---");

    // Analyze the complete Vehicle hierarchy
    Map<Class<?>, TypeStructureAST> vehicleHierarchy = MetaStage.analyzeSealedHierarchy(Vehicle.class);

    // Should contain all permitted types
    Set<Class<?>> expectedClasses = Set.of(Car.class, Truck.class, Motorcycle.class);
    assertTrue(vehicleHierarchy.keySet().containsAll(expectedClasses));

    // Each should be classified as RECORD
    for (Class<?> vehicleClass : expectedClasses) {
      TypeStructureAST ast = vehicleHierarchy.get(vehicleClass);
      assertEquals(StructuralTag.RECORD, ast.root().tag());
      assertTrue(ast.root().isUserDefined());
    }

    // Test deep component analysis for Car (most complex vehicle)
    Map<String, TypeStructureAST> carComponents = MetaStage.analyzeRecordComponents(Car.class);
    assertEquals(4, carComponents.size());

    // engine: Engine (nested record reference)
    TypeStructureAST engineAST = carComponents.get("engine");
    assertEquals(StructuralTag.RECORD, engineAST.root().tag());
    assertEquals(Engine.class, engineAST.root().type());

    LOGGER.finer(() -> String.format("  ✓ Vehicle hierarchy contains %d types%n", vehicleHierarchy.size()));
    LOGGER.finer(() -> "  ✓ All vehicle types correctly classified as records");
    LOGGER.finer(() -> "  ✓ Nested record references properly handled");
  }

  @Test
  @Order(6)
  @DisplayName("Integration Test 6: Performance and Caching Integration")
  void testPerformanceAndCachingIntegration() {
    LOGGER.finer(() -> "\n--- Testing Performance and Caching Integration ---");

    // Ensure cache is empty for meaningful performance test
    MetaStage.TYPE_CACHE.clear();

    // Perform complex analysis that should populate cache
    long startTime = System.nanoTime();
    Map<String, TypeStructureAST> fleetAnalysis = MetaStage.analyzeRecordComponents(Fleet.class);
    Map<Class<?>, TypeStructureAST> vehicleHierarchy = MetaStage.analyzeSealedHierarchy(Vehicle.class);
    long firstRunTime = System.nanoTime() - startTime;

    int cacheAfterFirst = MetaStage.TYPE_CACHE.size();
    assertTrue(cacheAfterFirst > 0, "Analysis should populate cache");

    // Second run should benefit from caching
    startTime = System.nanoTime();
    Map<String, TypeStructureAST> fleetAnalysis2 = MetaStage.analyzeRecordComponents(Fleet.class);
    Map<Class<?>, TypeStructureAST> vehicleHierarchy2 = MetaStage.analyzeSealedHierarchy(Vehicle.class);
    long secondRunTime = System.nanoTime() - startTime;

    // Validate caching is actually working
    assertTrue(secondRunTime < firstRunTime / 2, "Second run should be significantly faster");

    // Validate result consistency
    assertEquals(fleetAnalysis.size(), fleetAnalysis2.size());
    assertEquals(vehicleHierarchy.size(), vehicleHierarchy2.size());

    // Validate that cached results are actually the same instances
    for (String key : fleetAnalysis.keySet()) {
      assertSame(fleetAnalysis.get(key), fleetAnalysis2.get(key),
          "Cached AST instances should be identical");
    }

    LOGGER.finer(() -> String.format("  ✓ First run: %,d ns%n", firstRunTime));
    LOGGER.finer(() -> String.format("  ✓ Second run: %,d ns (%.1fx speedup)%n",
        secondRunTime, (double) firstRunTime / secondRunTime));
    LOGGER.finer(() -> String.format("  ✓ Cache size: %d -> %d entries%n", initialCacheSize, afterFirstRun));
  }

  @Test
  @Order(7)
  @DisplayName("Integration Test 7: Error Handling and Edge Cases")
  void testErrorHandlingAndEdgeCases() {
    LOGGER.finer(() -> "\n--- Testing Error Handling and Edge Cases ---");

    // Test null handling
    assertThrows(NullPointerException.class, () -> MetaStage.analyze(null));

    // Test invalid operations
    assertThrows(IllegalArgumentException.class,
        () -> MetaStage.analyzeRecordComponents(String.class));
    assertThrows(IllegalArgumentException.class,
        () -> MetaStage.analyzeSealedHierarchy(List.class));

    // Test validation edge cases
    TypeStructureAST emptyAST = new TypeStructureAST(Collections.emptyList());
    assertThrows(IllegalArgumentException.class,
        () -> MetaStage.validateSerializationSupport(emptyAST));

    // Test bounds checking
    TypeStructureAST validAST = MetaStage.analyze(String.class);
    assertThrows(IndexOutOfBoundsException.class, () -> validAST.substructure(-1));
    assertThrows(IndexOutOfBoundsException.class, () -> validAST.substructure(validAST.size()));

    // Test empty AST operations
    assertThrows(IllegalStateException.class, emptyAST::root);
    assertThrows(IllegalStateException.class, emptyAST::leaf);

    LOGGER.finer(() -> "  ✓ Null pointer protection working");
    LOGGER.finer(() -> "  ✓ Invalid operation detection working");
    LOGGER.finer(() -> "  ✓ Bounds checking working");
    LOGGER.finer(() -> "  ✓ Empty AST protection working");
  }

  @Test
  @Order(8)
  @DisplayName("Integration Test 8: README.md Example Validation")
  void testReadmeExampleValidation() {
    LOGGER.finer(() -> "\n--- Testing README.md Example Validation ---");

    // Validate all examples from README.md can be analyzed
    List<Class<?>> readmeClasses = List.of(
        // Basic examples
        String.class, int.class, UUID.class,
        // User-defined examples
        FuelType.class, Engine.class, Turbocharger.class,
        // Complex examples
        Car.class, Fleet.class
    );

    for (Class<?> clazz : readmeClasses) {
      assertDoesNotThrow(() -> {
        TypeStructureAST ast = MetaStage.analyze(clazz);
        MetaStage.validateSerializationSupport(ast);
      }, "README.md class should be analyzable: " + clazz.getSimpleName());
    }

    // Test specific README.md patterns
    Map<String, TypeStructureAST> fleetComponents = MetaStage.analyzeRecordComponents(Fleet.class);

    // vehicles: List<Vehicle> - demonstrates sealed interface in container
    TypeStructureAST vehiclesAST = fleetComponents.get("vehicles");
    assertEquals("LIST<INTERFACE>", vehiclesAST.toStructureString());

    // maintenanceRecords: Map<String, List<String>> - demonstrates nested containers
    TypeStructureAST recordsAST = fleetComponents.get("maintenanceRecords");
    assertEquals("MAP<STRING, LIST<STRING>>", recordsAST.toStructureString());

    LOGGER.finer(() -> "  ✓ All README.md classes analyzable");
    LOGGER.finer(() -> "  ✓ Sealed interfaces in containers working");
    LOGGER.finer(() -> "  ✓ Nested container patterns working");
  }

  @Test
  @Order(9)
  @DisplayName("Integration Test 9: Code Generation Readiness")
  void testCodeGenerationReadiness() {
    LOGGER.finer(() -> "\n--- Testing Code Generation Readiness ---");

    // Test that ASTs provide all information needed for code generation
    Map<String, TypeStructureAST> carComponents = MetaStage.analyzeRecordComponents(Car.class);

    for (Map.Entry<String, TypeStructureAST> entry : carComponents.entrySet()) {
      String componentName = entry.getKey();
      TypeStructureAST ast = entry.getValue();

      // Each component should provide clear serialization strategy
      assertTrue(ast.size() > 0, "AST should not be empty");
      assertNotNull(ast.root(), "Root tag should be accessible");
      assertNotNull(ast.leaf(), "Leaf tag should be accessible");

      // Structure should be unambiguous for code generation
      String structure = ast.toStructureString();
      assertFalse(structure.contains("null"), "Structure should not contain null references");

      // Should be able to determine delegation strategy
      if (ast.isSimple()) {
        // Simple types can be serialized directly
        assertTrue(ast.root().isLeaf(), "Simple types should have leaf root");
      } else {
        // Complex types need delegation chains
        assertTrue(ast.isNested(), "Complex types should be nested");
        assertTrue(ast.containerCount() > 0, "Complex types should have containers");
      }
    }

    // Test that map structures are properly formed for code generation
    try {
      Type mapField = Fleet.class.getDeclaredField("maintenanceRecords").getGenericType();
      TypeStructureAST mapAST = MetaStage.analyze(mapField);

      // Should contain exactly one MAP_SEPARATOR
      long separatorCount = mapAST.tagTypes().stream()
          .filter(tag -> tag.tag() == StructuralTag.MAP_SEPARATOR)
          .count();
      assertEquals(1, separatorCount, "Map should have exactly one separator");

    } catch (NoSuchFieldException e) {
      fail("Test field should exist: " + e.getMessage());
    }

    LOGGER.finer(() -> "  ✓ All ASTs provide complete serialization information");
    LOGGER.finer(() -> "  ✓ Simple vs complex type classification clear");
    LOGGER.finer(() -> "  ✓ Map structures properly formed");
    LOGGER.finer(() -> "  ✓ Delegation chain construction possible");
  }

  @Test
  @Order(10)
  @DisplayName("Integration Test 10: Complete System Validation")
  void testCompleteSystemValidation() {
    LOGGER.finer(() -> "\n--- Final Complete System Validation ---");

    // Test the complete workflow: meta-stage -> validation -> code generation readiness

    // 1. Meta-stage: Analyze the most complex type
    Map<String, TypeStructureAST> fleetComponents = MetaStage.analyzeRecordComponents(Fleet.class);
    Map<Class<?>, TypeStructureAST> vehicleHierarchy = MetaStage.analyzeSealedHierarchy(Vehicle.class);

    // 2. Validation: All ASTs should be serialization-ready
    fleetComponents.values().forEach(ast ->
        assertDoesNotThrow(() -> MetaStage.validateSerializationSupport(ast)));
    vehicleHierarchy.values().forEach(ast ->
        assertDoesNotThrow(() -> MetaStage.validateSerializationSupport(ast)));

    // 3. Code generation readiness: All structures should be unambiguous
    int totalTypes = fleetComponents.size() + vehicleHierarchy.size();
    int simpleTypes = 0;
    int complexTypes = 0;

    for (TypeStructureAST ast : fleetComponents.values()) {
      if (ast.isSimple()) simpleTypes++;
      else complexTypes++;
    }

    for (TypeStructureAST ast : vehicleHierarchy.values()) {
      if (ast.isSimple()) simpleTypes++;
      else complexTypes++;
    }

    // Make final copies for lambda usage
    final int finalSimpleTypes = simpleTypes;
    final int finalComplexTypes = complexTypes;

    // 4. Performance: System should be efficient
    assertTrue(MetaStage.getCacheSize() > 0, "Cache should be populated");

    // 5. Completeness: All README.md type patterns should be covered
    boolean hasArrays = fleetComponents.values().stream()
        .anyMatch(ast -> ast.toStructureString().contains("ARRAY"));
    boolean hasLists = fleetComponents.values().stream()
        .anyMatch(ast -> ast.toStructureString().contains("LIST"));
    boolean hasMaps = fleetComponents.values().stream()
        .anyMatch(ast -> ast.toStructureString().contains("MAP"));
    boolean hasOptionals = fleetComponents.values().stream()
        .anyMatch(ast -> ast.toStructureString().contains("OPTIONAL"));
    boolean hasRecords = vehicleHierarchy.values().stream()
        .anyMatch(ast -> ast.toStructureString().contains("RECORD"));
    boolean hasEnums = fleetComponents.values().stream()
        .anyMatch(ast -> ast.toStructureString().contains("ENUM"));
    boolean hasInterfaces = fleetComponents.values().stream()
        .anyMatch(ast -> ast.toStructureString().contains("INTERFACE"));

    LOGGER.finer(() -> String.format("  ✓ Total types analyzed: %d%n", totalTypes));
    LOGGER.finer(() -> String.format("  ✓ Simple types: %d, Complex types: %d%n", finalSimpleTypes, finalComplexTypes));
    LOGGER.finer(() -> String.format("  ✓ Cache entries: %d%n", MetaStage.getCacheSize()));
    LOGGER.finer(() -> "  ✓ Type pattern coverage:");
    LOGGER.finer(() -> String.format("    Arrays: %b, Lists: %b, Maps: %b, Optionals: %b%n",
        hasArrays, hasLists, hasMaps, hasOptionals));
    LOGGER.finer(() -> String.format("    Records: %b, Enums: %b, Interfaces: %b%n",
        hasRecords, hasEnums, hasInterfaces));

    // Final validation: The system is ready for integration
    assertTrue(totalTypes > 0, "System should have analyzed types");
    assertTrue(hasLists && hasMaps && hasRecords, "Core patterns should be present");

    LOGGER.finer(() -> "\n  🎉 COMPLETE SYSTEM VALIDATION SUCCESSFUL 🎉");
    LOGGER.finer(() -> "  The AST construction system is ready for integration");
    LOGGER.finer(() -> "  with the No Framework Pickler metaprogramming pipeline.");
  }
}
