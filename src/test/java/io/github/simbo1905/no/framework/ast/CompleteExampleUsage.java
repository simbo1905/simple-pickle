package io.github.simbo1905.no.framework.ast;
import static io.github.simbo1905.no.framework.Pickler.LOGGER;

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
    LOGGER.finer(() -> "=== No Framework Pickler: AST Construction Demo ===\n");

    // Stage 1: Meta-stage Analysis - Build ASTs for all components
    demonstrateBasicTypeAnalysis();
    demonstrateContainerAnalysis();
    demonstrateUserDefinedTypeAnalysis();
    demonstrateComplexNestedAnalysis();
    demonstrateSealedHierarchyAnalysis();
    demonstratePerformanceAnalysis();

    LOGGER.finer(() -> "\n=== Analysis Complete: Ready for Code Generation ===");
    LOGGER.finer(() -> "The ASTs constructed above would now be used to generate");
    LOGGER.finer(() -> "specialized serialization code for zero-reflection runtime performance.");
  }

  static void demonstrateBasicTypeAnalysis() {
    LOGGER.finer(() -> "1. Basic Type Analysis (Primitive and Built-in Types)");
    LOGGER.finer(() -> "==================================================");

    // Analyze primitive types
    TypeStructureAST intAST = MetaStage.analyze(int.class);
    TypeStructureAST stringAST = MetaStage.analyze(String.class);
    TypeStructureAST uuidAST = MetaStage.analyze(UUID.class);

    LOGGER.finer(() -> String.format("  int.class     -> %s (simple: %b)%n", intAST.toStructureString(), intAST.isSimple()));
    LOGGER.finer(() -> String.format("  String.class  -> %s (simple: %b)%n", stringAST.toStructureString(), stringAST.isSimple()));
    LOGGER.finer(() -> String.format("  UUID.class    -> %s (simple: %b)%n", uuidAST.toStructureString(), uuidAST.isSimple()));

    // Validate all are serialization-ready
    MetaStage.validateSerializationSupport(intAST);
    MetaStage.validateSerializationSupport(stringAST);
    MetaStage.validateSerializationSupport(uuidAST);

    LOGGER.finer(() -> "  ✓ All basic types validated for serialization\n");
  }

  static void demonstrateContainerAnalysis() {
    LOGGER.finer(() -> "2. Container Type Analysis (Arrays, Lists, Optionals, Maps)");
    LOGGER.finer(() -> "=========================================================");

    // Analyze container types
    TypeStructureAST arrayAST = MetaStage.analyze(String[].class);
    LOGGER.finer(() -> String.format("  String[]      -> %s (containers: %d)%n",
        arrayAST.toStructureString(), arrayAST.containerCount()));

    // For generic types, we need to use reflection field access
    try {
      var field = CompleteExampleUsage.class.getDeclaredField("exampleList");
      TypeStructureAST listAST = MetaStage.analyze(field.getGenericType());
      LOGGER.finer(() -> String.format("  List<String>  -> %s (containers: %d)%n",
          listAST.toStructureString(), listAST.containerCount()));

      field = CompleteExampleUsage.class.getDeclaredField("exampleOptional");
      TypeStructureAST optionalAST = MetaStage.analyze(field.getGenericType());
      LOGGER.finer(() -> String.format("  Optional<Int> -> %s (containers: %d)%n",
          optionalAST.toStructureString(), optionalAST.containerCount()));

      field = CompleteExampleUsage.class.getDeclaredField("exampleMap");
      TypeStructureAST mapAST = MetaStage.analyze(field.getGenericType());
      LOGGER.finer(() -> String.format("  Map<Str,Int>  -> %s (containers: %d)%n",
          mapAST.toStructureString(), mapAST.containerCount()));

      LOGGER.finer(() -> "  ✓ All container types analyzed successfully\n");

    } catch (NoSuchFieldException e) {
      LOGGER.warning(() -> "  Error accessing example fields: " + e.getMessage());
    }
  }

  static void demonstrateUserDefinedTypeAnalysis() {
    LOGGER.finer(() -> "3. User-Defined Type Analysis (Records, Enums, Interfaces)");
    LOGGER.finer(() -> "========================================================");

    // Analyze user-defined types
    TypeStructureAST personAST = MetaStage.analyze(Person.class);
    TypeStructureAST treeEnumAST = MetaStage.analyze(TreeEnum.class);
    TypeStructureAST treeInterfaceAST = MetaStage.analyze(TreeNode.class);

    LOGGER.finer(() -> String.format("  Person.class    -> %s%n", personAST.toStructureString()));
    LOGGER.finer(() -> String.format("  TreeEnum.class  -> %s%n", treeEnumAST.toStructureString()));
    LOGGER.finer(() -> String.format("  TreeNode.class  -> %s%n", treeInterfaceAST.toStructureString()));

    // Analyze record components
    Map<String, TypeStructureAST> personComponents = MetaStage.analyzeRecordComponents(Person.class);
    LOGGER.finer(() -> "  Person record components:");
    personComponents.forEach((name, ast) ->
        LOGGER.finer(() -> String.format("    %s: %s%n", name, ast.toStructureString())));

    LOGGER.finer(() -> "  ✓ All user-defined types analyzed successfully\n");
  }

  static void demonstrateComplexNestedAnalysis() {
    LOGGER.finer(() -> "4. Complex Nested Structure Analysis");
    LOGGER.finer(() -> "===================================");

    // Analyze the complex record and its components
    Map<String, TypeStructureAST> complexComponents = MetaStage.analyzeRecordComponents(ComplexRecord.class);

    LOGGER.finer(() -> "  ComplexRecord components:");
    complexComponents.forEach((name, ast) -> {
      LOGGER.finer(() -> String.format("    %-20s: %s%n", name, ast.toStructureString()));
      LOGGER.finer(() -> String.format("    %-20s  containers: %d, nested: %b%n",
          "", ast.containerCount(), ast.isNested()));
    });

    // Demonstrate substructure analysis for complex nested types
    TypeStructureAST nestedStructure = complexComponents.get("nestedStructure");
    if (nestedStructure.size() > 2) {
      TypeStructureAST subStructure = nestedStructure.substructure(1);
      LOGGER.finer(() -> String.format("    Substructure from index 1: %s%n", subStructure.toStructureString()));
    }

    LOGGER.finer(() -> "  ✓ Complex nested structures analyzed successfully\n");
  }

  static void demonstrateSealedHierarchyAnalysis() {
    LOGGER.finer(() -> "5. Sealed Interface Hierarchy Analysis");
    LOGGER.finer(() -> "====================================");

    // Analyze the complete sealed hierarchy
    Map<Class<?>, TypeStructureAST> hierarchy = MetaStage.analyzeSealedHierarchy(TreeNode.class);

    LOGGER.finer(() -> "  TreeNode sealed hierarchy:");
    hierarchy.forEach((clazz, ast) -> {
      LOGGER.finer(() -> String.format("    %-20s: %s%n", clazz.getSimpleName(), ast.toStructureString()));

      // For record types, show their components
      if (clazz.isRecord()) {
        Map<String, TypeStructureAST> components = MetaStage.analyzeRecordComponents(clazz);
        components.forEach((name, componentAST) ->
            LOGGER.finer(() -> String.format("      %-18s: %s%n", name, componentAST.toStructureString())));
      }
    });

    LOGGER.finer(() -> "  ✓ Sealed hierarchy analyzed successfully\n");
  }

  static void demonstratePerformanceAnalysis() {
    LOGGER.finer(() -> "6. Performance and Caching Analysis");
    LOGGER.finer(() -> "=================================");

    LOGGER.finer(() -> String.format("  Cache size before: %d%n", MetaStage.getCacheSize()));

    // Perform multiple analyses to test caching
    long startTime = System.nanoTime();
    TypeStructureAST ast1 = MetaStage.analyze(ComplexRecord.class);
    long firstAnalysis = System.nanoTime() - startTime;

    startTime = System.nanoTime();
    TypeStructureAST ast2 = MetaStage.analyze(ComplexRecord.class);
    long secondAnalysis = System.nanoTime() - startTime;

    LOGGER.finer(() -> String.format("  Cache size after: %d%n", MetaStage.getCacheSize()));
    LOGGER.finer(() -> String.format("  First analysis:  %,d ns%n", firstAnalysis));
    LOGGER.finer(() -> String.format("  Second analysis: %,d ns%n", secondAnalysis));
    LOGGER.finer(() -> String.format("  Cache speedup:   %.1fx%n", (double) firstAnalysis / secondAnalysis));
    LOGGER.finer(() -> String.format("  Same instance:   %b%n", ast1 == ast2));

    // Analyze performance with complex nested structures
    startTime = System.nanoTime();
    Map<String, TypeStructureAST> components = MetaStage.analyzeRecordComponents(ComplexRecord.class);
    long componentAnalysis = System.nanoTime() - startTime;

    LOGGER.finer(() -> String.format("  Component analysis: %,d ns (%d components)%n",
        componentAnalysis, components.size()));

    LOGGER.finer(() -> "  ✓ Performance analysis complete\n");
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

  static String generateComponentSerialization(String componentName, TypeStructureAST ast, int indent) {
    String indentStr = "  ".repeat(indent);
    StringBuilder code = new StringBuilder();

    if (ast.isSimple()) {
      // Simple types can be serialized directly
      TagWithType tag = ast.root();
      switch (tag.tag()) {
        case STRING -> code.append(String.format("%swriteString(buffer, instance.%s());%n", indentStr, componentName));
        case INTEGER -> code.append(String.format("%swriteInt(buffer, instance.%s());%n", indentStr, componentName));
        case BOOLEAN -> code.append(String.format("%swriteBoolean(buffer, instance.%s());%n", indentStr, componentName));
        case RECORD -> {
          String className = tag.type() instanceof Class<?> clazz ? clazz.getSimpleName() : tag.type().getTypeName();
          code.append(String.format("%s// Delegate to %s serializer%n%sserialize%s(buffer, instance.%s());%n",
              indentStr, className, indentStr, className, componentName));
        }
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
  List<String> exampleList;
  @SuppressWarnings("unused")
  Optional<Integer> exampleOptional;
  @SuppressWarnings("unused")
  Map<String, Integer> exampleMap;
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
    LOGGER.finer(() -> "=== AST Integration with No Framework Pickler ===\n");

    // This is where the AST construction would integrate with the existing
    // TypeStructure.analyze() method mentioned in the requirements

    LOGGER.finer(() -> "1. Legacy TypeStructure.analyze() method would be replaced by:");
    LOGGER.finer(() -> "   MetaStage.analyze(Type) -> TypeStructureAST");
    LOGGER.finer(() -> "");

    LOGGER.finer(() -> "2. The existing parallel lists (tags, types) would be replaced by:");
    LOGGER.finer(() -> "   TypeStructureAST.tagTypes() -> List<TagWithType>");
    LOGGER.finer(() -> "");

    LOGGER.finer(() -> "3. The AST enables more sophisticated analysis:");
    LOGGER.finer(() -> "   - Component-level analysis for records");
    LOGGER.finer(() -> "   - Sealed hierarchy discovery");
    LOGGER.finer(() -> "   - Static validation of serialization support");
    LOGGER.finer(() -> "   - Performance optimization through caching");
    LOGGER.finer(() -> "");

    LOGGER.finer(() -> "4. Code generation becomes more structured:");
    LOGGER.finer(() -> "   - AST nodes map directly to serialization operations");
    LOGGER.finer(() -> "   - Delegation chains are explicit in the tree structure");
    LOGGER.finer(() -> "   - Right-to-left construction follows leaf-to-root traversal");
    LOGGER.finer(() -> "");

    // Example of how existing code would be updated
    LOGGER.finer(() -> "5. Migration path from existing code:");
    LOGGER.finer(() -> "   OLD: TypeStructure structure = TypeStructure.analyze(componentType);");
    LOGGER.finer(() -> "   NEW: TypeStructureAST ast = MetaStage.analyze(componentType);");
    LOGGER.finer(() -> "        MetaStage.validateSerializationSupport(ast);");
    LOGGER.finer(() -> "");

    LOGGER.finer(() -> "=== Integration Analysis Complete ===");
  }

  public static void main(String[] args) {
    demonstrateIntegration();
  }
}
