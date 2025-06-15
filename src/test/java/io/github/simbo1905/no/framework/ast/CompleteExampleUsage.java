package io.github.simbo1905.no.framework.ast;

import java.lang.reflect.RecordComponent;
import java.util.*;

/**
 * Complete example demonstrating the integration of AST construction,
 * staged metaprogramming, and static type analysis for the No Framework Pickler.
 *
 * This class shows how the academic concepts translate into practical
 * implementation for compile-time code generation and zero-reflection serialization.
 */
public class CompleteExampleUsage {

  // Example types from README.md
  public sealed interface TreeNode permits InternalNode, LeafNode, TreeEnum {
    static TreeNode empty() { return TreeEnum.EMPTY; }
  }

  public record InternalNode(String name, TreeNode left, TreeNode right) implements TreeNode {}
  public record LeafNode(int value) implements TreeNode {}
  public enum TreeEnum implements TreeNode { EMPTY }

  public record ComplexRecord(
      String name,
      List<Optional<String[]>> nestedStructure,
      Map<UUID, List<Person>> complexMap,
      TreeNode treeReference
  ) {}

  public record Person(String name, int age) {}

  /**
   * Demonstrates the complete multi-stage programming workflow:
   * 1. Meta-stage: Analyze types and construct ASTs
   * 2. Code generation stage: Use ASTs to generate specialized serializers
   * 3. Object-stage: Execute generated code without reflection
   */
  public static void main(String[] args) {
    System.out.println("=== No Framework Pickler: AST Construction Demo ===\n");

    // Stage 1: Meta-stage Analysis - Build ASTs for all components
    demonstrateBasicTypeAnalysis();
    demonstrateContainerAnalysis();
    demonstrateUserDefinedTypeAnalysis();
    demonstrateComplexNestedAnalysis();
    demonstrateSealedHierarchyAnalysis();
    demonstratePerformanceAnalysis();

    System.out.println("\n=== Analysis Complete: Ready for Code Generation ===");
    System.out.println("The ASTs constructed above would now be used to generate");
    System.out.println("specialized serialization code for zero-reflection runtime performance.");
  }

  private static void demonstrateBasicTypeAnalysis() {
    System.out.println("1. Basic Type Analysis (Primitive and Built-in Types)");
    System.out.println("==================================================");

    // Analyze primitive types
    TypeStructureAST intAST = MetaStage.analyze(int.class);
    TypeStructureAST stringAST = MetaStage.analyze(String.class);
    TypeStructureAST uuidAST = MetaStage.analyze(UUID.class);

    System.out.printf("  int.class     -> %s (simple: %b)%n", intAST.toStructureString(), intAST.isSimple());
    System.out.printf("  String.class  -> %s (simple: %b)%n", stringAST.toStructureString(), stringAST.isSimple());
    System.out.printf("  UUID.class    -> %s (simple: %b)%n", uuidAST.toStructureString(), uuidAST.isSimple());

    // Validate all are serialization-ready
    MetaStage.validateSerializationSupport(intAST);
    MetaStage.validateSerializationSupport(stringAST);
    MetaStage.validateSerializationSupport(uuidAST);

    System.out.println("  ✓ All basic types validated for serialization\n");
  }

  private static void demonstrateContainerAnalysis() {
    System.out.println("2. Container Type Analysis (Arrays, Lists, Optionals, Maps)");
    System.out.println("=========================================================");

    // Analyze container types
    TypeStructureAST arrayAST = MetaStage.analyze(String[].class);
    System.out.printf("  String[]      -> %s (containers: %d)%n",
        arrayAST.toStructureString(), arrayAST.containerCount());

    // For generic types, we need to use reflection field access
    try {
      var field = CompleteExampleUsage.class.getDeclaredField("exampleList");
      TypeStructureAST listAST = MetaStage.analyze(field.getGenericType());
      System.out.printf("  List<String>  -> %s (containers: %d)%n",
          listAST.toStructureString(), listAST.containerCount());

      field = CompleteExampleUsage.class.getDeclaredField("exampleOptional");
      TypeStructureAST optionalAST = MetaStage.analyze(field.getGenericType());
      System.out.printf("  Optional<Int> -> %s (containers: %d)%n",
          optionalAST.toStructureString(), optionalAST.containerCount());

      field = CompleteExampleUsage.class.getDeclaredField("exampleMap");
      TypeStructureAST mapAST = MetaStage.analyze(field.getGenericType());
      System.out.printf("  Map<Str,Int>  -> %s (containers: %d)%n",
          mapAST.toStructureString(), mapAST.containerCount());

      System.out.println("  ✓ All container types analyzed successfully\n");

    } catch (NoSuchFieldException e) {
      System.err.println("  Error accessing example fields: " + e.getMessage());
    }
  }

  private static void demonstrateUserDefinedTypeAnalysis() {
    System.out.println("3. User-Defined Type Analysis (Records, Enums, Interfaces)");
    System.out.println("========================================================");

    // Analyze user-defined types
    TypeStructureAST personAST = MetaStage.analyze(Person.class);
    TypeStructureAST treeEnumAST = MetaStage.analyze(TreeEnum.class);
    TypeStructureAST treeInterfaceAST = MetaStage.analyze(TreeNode.class);

    System.out.printf("  Person.class    -> %s%n", personAST.toStructureString());
    System.out.printf("  TreeEnum.class  -> %s%n", treeEnumAST.toStructureString());
    System.out.printf("  TreeNode.class  -> %s%n", treeInterfaceAST.toStructureString());

    // Analyze record components
    Map<String, TypeStructureAST> personComponents = MetaStage.analyzeRecordComponents(Person.class);
    System.out.println("  Person record components:");
    personComponents.forEach((name, ast) ->
        System.out.printf("    %s: %s%n", name, ast.toStructureString()));

    System.out.println("  ✓ All user-defined types analyzed successfully\n");
  }

  private static void demonstrateComplexNestedAnalysis() {
    System.out.println("4. Complex Nested Structure Analysis");
    System.out.println("===================================");

    // Analyze the complex record and its components
    Map<String, TypeStructureAST> complexComponents = MetaStage.analyzeRecordComponents(ComplexRecord.class);

    System.out.println("  ComplexRecord components:");
    complexComponents.forEach((name, ast) -> {
      System.out.printf("    %-20s: %s%n", name, ast.toStructureString());
      System.out.printf("    %-20s  containers: %d, nested: %b%n",
          "", ast.containerCount(), ast.isNested());
    });

    // Demonstrate substructure analysis for complex nested types
    TypeStructureAST nestedStructure = complexComponents.get("nestedStructure");
    if (nestedStructure.size() > 2) {
      TypeStructureAST subStructure = nestedStructure.substructure(1);
      System.out.printf("    Substructure from index 1: %s%n", subStructure.toStructureString());
    }

    System.out.println("  ✓ Complex nested structures analyzed successfully\n");
  }

  private static void demonstrateSealedHierarchyAnalysis() {
    System.out.println("5. Sealed Interface Hierarchy Analysis");
    System.out.println("====================================");

    // Analyze the complete sealed hierarchy
    Map<Class<?>, TypeStructureAST> hierarchy = MetaStage.analyzeSealedHierarchy(TreeNode.class);

    System.out.println("  TreeNode sealed hierarchy:");
    hierarchy.forEach((clazz, ast) -> {
      System.out.printf("    %-20s: %s%n", clazz.getSimpleName(), ast.toStructureString());

      // For record types, show their components
      if (clazz.isRecord()) {
        Map<String, TypeStructureAST> components = MetaStage.analyzeRecordComponents(clazz);
        components.forEach((name, componentAST) ->
            System.out.printf("      %-18s: %s%n", name, componentAST.toStructureString()));
      }
    });

    System.out.println("  ✓ Sealed hierarchy analyzed successfully\n");
  }

  private static void demonstratePerformanceAnalysis() {
    System.out.println("6. Performance and Caching Analysis");
    System.out.println("=================================");

    System.out.printf("  Cache size before: %d%n", MetaStage.getCacheSize());

    // Perform multiple analyses to test caching
    long startTime = System.nanoTime();
    TypeStructureAST ast1 = MetaStage.analyze(ComplexRecord.class);
    long firstAnalysis = System.nanoTime() - startTime;

    startTime = System.nanoTime();
    TypeStructureAST ast2 = MetaStage.analyze(ComplexRecord.class);
    long secondAnalysis = System.nanoTime() - startTime;

    System.out.printf("  Cache size after: %d%n", MetaStage.getCacheSize());
    System.out.printf("  First analysis:  %,d ns%n", firstAnalysis);
    System.out.printf("  Second analysis: %,d ns%n", secondAnalysis);
    System.out.printf("  Cache speedup:   %.1fx%n", (double) firstAnalysis / secondAnalysis);
    System.out.printf("  Same instance:   %b%n", ast1 == ast2);

    // Analyze performance with complex nested structures
    startTime = System.nanoTime();
    Map<String, TypeStructureAST> components = MetaStage.analyzeRecordComponents(ComplexRecord.class);
    long componentAnalysis = System.nanoTime() - startTime;

    System.out.printf("  Component analysis: %,d ns (%d components)%n",
        componentAnalysis, components.size());

    System.out.println("  ✓ Performance analysis complete\n");
  }

  /**
   * Demonstration of how the AST would be used for code generation.
   * This shows the conceptual bridge between meta-stage analysis and object-stage execution.
   */
  public static String generateSerializationCode(String className, Map<String, TypeStructureAST> components) {
    StringBuilder codeBuilder = new StringBuilder();

    codeBuilder.append(String.format("// Generated serializer for %s%n", className));
    codeBuilder.append(String.format("public void serialize(ByteBuffer buffer, %s instance) {%n", className));

    for (Map.Entry<String, TypeStructureAST> entry : components.entrySet()) {
      String componentName = entry.getKey();
      TypeStructureAST ast = entry.getValue();

      codeBuilder.append(generateComponentSerialization(componentName, ast, 1));
    }

    codeBuilder.append("}%n");

    return codeBuilder.toString();
  }

  private static String generateComponentSerialization(String componentName, TypeStructureAST ast, int indent) {
    String indentStr = "  ".repeat(indent);
    StringBuilder code = new StringBuilder();

    if (ast.isSimple()) {
      // Simple types can be serialized directly
      TagWithType tag = ast.root();
      switch (tag.tag()) {
        case STRING -> code.append(String.format("%swriteString(buffer, instance.%s());%n", indentStr, componentName));
        case INTEGER -> code.append(String.format("%swriteInt(buffer, instance.%s());%n", indentStr, componentName));
        case BOOLEAN -> code.append(String.format("%swriteBoolean(buffer, instance.%s());%n", indentStr, componentName));
        case RECORD -> code.append(String.format("%s// Delegate to %s serializer%n%sserialize%s(buffer, instance.%s());%n",
            indentStr, tag.type().getSimpleName(), indentStr, tag.type().getSimpleName(), componentName));
        case ENUM -> code.append(String.format("%swriteEnum(buffer, instance.%s());%n", indentStr, componentName));
        default -> code.append(String.format("%s// Serialize %s: %s%n", indentStr, componentName, ast.toStructureString()));
      }
    } else {
      // Complex types require container handling
      code.append(String.format("%s// Serialize %s: %s%n", indentStr, componentName, ast.toStructureString()));
      code.append(String.format("%s// TODO: Generate container delegation chain%n", indentStr));
    }

    return code.toString();
  }

  // Example fields for reflection-based generic type access
  @SuppressWarnings("unused")
  private List<String> exampleList;
  @SuppressWarnings("unused")
  private Optional<Integer> exampleOptional;
  @SuppressWarnings("unused")
  private Map<String, Integer> exampleMap;
}

/**
 * Integration utility that demonstrates how the AST construction integrates
 * with the broader No Framework Pickler architecture.
 */
class ASTIntegrationDemo {

  /**
   * Simulates the integration point where AST analysis feeds into
   * the existing PicklerImpl code generation.
   */
  public static void demonstrateIntegration() {
    System.out.println("=== AST Integration with No Framework Pickler ===\n");

    // This is where the AST construction would integrate with the existing
    // TypeStructure.analyze() method mentioned in the requirements

    System.out.println("1. Legacy TypeStructure.analyze() method would be replaced by:");
    System.out.println("   MetaStage.analyze(Type) -> TypeStructureAST");
    System.out.println();

    System.out.println("2. The existing parallel lists (tags, types) would be replaced by:");
    System.out.println("   TypeStructureAST.tagTypes() -> List<TagWithType>");
    System.out.println();

    System.out.println("3. The AST enables more sophisticated analysis:");
    System.out.println("   - Component-level analysis for records");
    System.out.println("   - Sealed hierarchy discovery");
    System.out.println("   - Static validation of serialization support");
    System.out.println("   - Performance optimization through caching");
    System.out.println();

    System.out.println("4. Code generation becomes more structured:");
    System.out.println("   - AST nodes map directly to serialization operations");
    System.out.println("   - Delegation chains are explicit in the tree structure");
    System.out.println("   - Right-to-left construction follows leaf-to-root traversal");
    System.out.println();

    // Example of how existing code would be updated
    System.out.println("5. Migration path from existing code:");
    System.out.println("   OLD: TypeStructure structure = TypeStructure.analyze(componentType);");
    System.out.println("   NEW: TypeStructureAST ast = MetaStage.analyze(componentType);");
    System.out.println("        MetaStage.validateSerializationSupport(ast);");
    System.out.println();

    System.out.println("=== Integration Analysis Complete ===");
  }

  public static void main(String[] args) {
    demonstrateIntegration();
  }
}
