package io.github.simbo1905.no.framework.ast;

import java.lang.reflect.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Multi-stage programming implementation for compile-time AST construction and analysis.
 *
 * This class implements the meta-stage of the two-stage computation model:
 * 1. Meta-stage (Construction Time): Static analysis builds specialized AST structures
 * 2. Object-stage (Runtime): Generated delegation chains execute without reflection
 *
 * The MetaStage performs recursive descent parsing of Java's Type hierarchy,
 * constructing Abstract Syntax Trees that enable zero-reflection serialization
 * through staged code generation.
 */
public final class MetaStage {

  /**
   * Cache for analyzed type structures to avoid redundant analysis.
   * This implements memoization for the expensive reflection-based analysis.
   */
  private static final Map<Type, TypeStructureAST> TYPE_CACHE = new ConcurrentHashMap<>();

  /**
   * Set of types currently being analyzed to detect and handle circular references.
   */
  private static final ThreadLocal<Set<Type>> ANALYSIS_STACK = ThreadLocal.withInitial(HashSet::new);

  /**
   * Analyzes a Java Type and constructs its complete AST representation.
   *
   * This method implements recursive descent parsing of Java's parameterized type system,
   * building an Abstract Syntax Tree that represents the nested container structure.
   * The resulting AST enables compile-time specialization through multi-stage programming.
   *
   * Algorithm:
   * 1. Container Recognition: Identifies parameterized types and arrays
   * 2. Recursive Decomposition: Recursively analyzes type arguments to arbitrary depth
   * 3. AST Construction: Builds flattened sequence of structural tags and concrete types
   * 4. Termination: Reaches leaf nodes at primitive/user-defined types
   *
   * The analysis produces type structures conforming to the formal grammar:
   * TypeStructure ::= { ContainerTag TypeClass }* PrimitiveTag PrimitiveClass
   *
   * Special case for Map<K,V>:
   * MapStructure ::= MAP { KeyStructure } MAP_SEPARATOR { ValueStructure }
   *
   * @param type Java Type to analyze (from RecordComponent.getGenericType())
   * @return TypeStructureAST representing the complete container hierarchy
   * @throws IllegalArgumentException if the type contains unsupported structures
   * @throws StackOverflowError if circular type references are detected
   */
  public static TypeStructureAST analyze(Type type) {
    Objects.requireNonNull(type, "Type cannot be null");

    // Check cache first for performance optimization
    TypeStructureAST cached = TYPE_CACHE.get(type);
    if (cached != null) {
      return cached;
    }

    // Detect circular references
    Set<Type> currentStack = ANALYSIS_STACK.get();
    if (currentStack.contains(type)) {
      throw new IllegalArgumentException("Circular type reference detected: " + type);
    }

    try {
      currentStack.add(type);
      TypeStructureAST result = performAnalysis(type);
      TYPE_CACHE.put(type, result);
      return result;
    } finally {
      currentStack.remove(type);
    }
  }

  /**
   * Performs the actual recursive descent analysis of a Java Type.
   * This is the core implementation of the AST construction algorithm.
   */
  private static TypeStructureAST performAnalysis(Type type) {
    List<TagWithType> tagTypes = new ArrayList<>();
    analyzeTypeRecursively(type, tagTypes);
    return new TypeStructureAST(tagTypes);
  }

  /**
   * Recursive descent parser for Java parameterized types.
   *
   * This method implements the core algorithm for AST construction:
   * - Iteratively unwraps container types (List, Map, Array, Optional)
   * - Adds container tags and marker classes to the AST sequence
   * - Recursively analyzes type arguments
   * - Terminates at primitive/user-defined leaf types
   */
  private static void analyzeTypeRecursively(Type type, List<TagWithType> tagTypes) {

    // Handle arrays first (both primitive arrays and object arrays)
    if (type instanceof Class<?> clazz && clazz.isArray()) {
      tagTypes.add(TagWithType.container(StructuralTag.ARRAY, clazz));
      analyzeTypeRecursively(clazz.getComponentType(), tagTypes);
      return;
    }

    // Handle generic array types (e.g., T[] where T is a type parameter)
    if (type instanceof GenericArrayType genericArrayType) {
      tagTypes.add(TagWithType.container(StructuralTag.ARRAY, genericArrayType));
      analyzeTypeRecursively(genericArrayType.getGenericComponentType(), tagTypes);
      return;
    }

    // Handle parameterized types (List<T>, Map<K,V>, Optional<T>)
    if (type instanceof ParameterizedType paramType) {
      Type rawType = paramType.getRawType();

      if (rawType instanceof Class<?> rawClass) {
        Type[] typeArgs = paramType.getActualTypeArguments();

        // Handle List<T>
        if (List.class.isAssignableFrom(rawClass)) {
          tagTypes.add(TagWithType.container(StructuralTag.LIST, rawClass));
          if (typeArgs.length == 1) {
            analyzeTypeRecursively(typeArgs[0], tagTypes);
          } else {
            throw new IllegalArgumentException("List must have exactly one type argument: " + type);
          }
          return;
        }

        // Handle Optional<T>
        if (Optional.class.isAssignableFrom(rawClass)) {
          tagTypes.add(TagWithType.container(StructuralTag.OPTIONAL, rawClass));
          if (typeArgs.length == 1) {
            analyzeTypeRecursively(typeArgs[0], tagTypes);
          } else {
            throw new IllegalArgumentException("Optional must have exactly one type argument: " + type);
          }
          return;
        }

        // Handle Map<K,V> - Special case requiring key and value analysis
        if (Map.class.isAssignableFrom(rawClass)) {
          tagTypes.add(TagWithType.container(StructuralTag.MAP, rawClass));
          if (typeArgs.length == 2) {
            // Analyze key type
            analyzeTypeRecursively(typeArgs[0], tagTypes);
            // Add separator marker
            tagTypes.add(TagWithType.mapSeparator());
            // Analyze value type
            analyzeTypeRecursively(typeArgs[1], tagTypes);
          } else {
            throw new IllegalArgumentException("Map must have exactly two type arguments: " + type);
          }
          return;
        }
      }
    }

    // Handle raw classes (primitive types, records, enums, interfaces)
    if (type instanceof Class<?> clazz) {
      StructuralTag tag = classifyClass(clazz);

      if (tag.name().startsWith("PRIMITIVE")) {
        // This should not happen as we handle primitives by name
        throw new IllegalStateException("Unexpected primitive classification: " + clazz);
      }

      switch (tag) {
        case BOOLEAN, BYTE, SHORT, CHARACTER, INTEGER, LONG, FLOAT, DOUBLE, STRING, UUID ->
          tagTypes.add(TagWithType.primitive(tag, clazz));
        case ENUM ->
          tagTypes.add(TagWithType.userDefined(tag, clazz));
        case RECORD -> {
          // For records, we just mark the record type - component analysis happens elsewhere
          tagTypes.add(TagWithType.userDefined(tag, clazz));
        }
        case INTERFACE -> {
          // For sealed interfaces, we mark the interface type
          tagTypes.add(TagWithType.userDefined(tag, clazz));
        }
        default ->
          throw new IllegalArgumentException("Unsupported class type: " + clazz);
      }
      return;
    }

    // Handle type variables and wildcards
    if (type instanceof TypeVariable<?>) {
      throw new IllegalArgumentException("Type variables are not supported in serialization: " + type);
    }

    if (type instanceof WildcardType) {
      throw new IllegalArgumentException("Wildcard types are not supported in serialization: " + type);
    }

    throw new IllegalArgumentException("Unsupported type: " + type + " of class " + type.getClass());
  }

  /**
   * Classifies a Java Class into the appropriate StructuralTag.
   * This method implements the static semantic analysis for leaf types.
   */
  private static StructuralTag classifyClass(Class<?> clazz) {
    // Handle primitive types and their boxed equivalents
    if (clazz == boolean.class || clazz == Boolean.class) {
      return StructuralTag.BOOLEAN;
    }
    if (clazz == byte.class || clazz == Byte.class) {
      return StructuralTag.BYTE;
    }
    if (clazz == short.class || clazz == Short.class) {
      return StructuralTag.SHORT;
    }
    if (clazz == char.class || clazz == Character.class) {
      return StructuralTag.CHARACTER;
    }
    if (clazz == int.class || clazz == Integer.class) {
      return StructuralTag.INTEGER;
    }
    if (clazz == long.class || clazz == Long.class) {
      return StructuralTag.LONG;
    }
    if (clazz == float.class || clazz == Float.class) {
      return StructuralTag.FLOAT;
    }
    if (clazz == double.class || clazz == Double.class) {
      return StructuralTag.DOUBLE;
    }

    // Handle reference types
    if (clazz == String.class) {
      return StructuralTag.STRING;
    }
    if (clazz == java.util.UUID.class) {
      return StructuralTag.UUID;
    }

    // Handle user-defined types
    if (clazz.isEnum()) {
      return StructuralTag.ENUM;
    }
    if (clazz.isRecord()) {
      return StructuralTag.RECORD;
    }
    if (clazz.isInterface() || clazz.isSealed()) {
      return StructuralTag.INTERFACE;
    }

    throw new IllegalArgumentException("Unsupported class type: " + clazz);
  }

  /**
   * Analyzes all components of a record class, returning individual ASTs for each component.
   * This method performs comprehensive static analysis of record structure for metaprogramming.
   *
   * @param recordClass The record class to analyze
   * @return Map from component name to its TypeStructureAST
   * @throws IllegalArgumentException if the class is not a record
   */
  public static Map<String, TypeStructureAST> analyzeRecordComponents(Class<?> recordClass) {
    if (!recordClass.isRecord()) {
      throw new IllegalArgumentException("Class must be a record: " + recordClass);
    }

    RecordComponent[] components = recordClass.getRecordComponents();
    Map<String, TypeStructureAST> componentStructures = new LinkedHashMap<>();

    for (RecordComponent component : components) {
      Type genericType = component.getGenericType();
      TypeStructureAST structure = analyze(genericType);
      componentStructures.put(component.getName(), structure);
    }

    return componentStructures;
  }

  /**
   * Analyzes a sealed interface hierarchy, discovering all permitted types and their structures.
   * This enables compile-time verification and optimization of sealed interface serialization.
   *
   * @param sealedInterface The sealed interface to analyze
   * @return Map from permitted class to its TypeStructureAST
   * @throws IllegalArgumentException if the interface is not sealed
   */
  public static Map<Class<?>, TypeStructureAST> analyzeSealedHierarchy(Class<?> sealedInterface) {
    if (!sealedInterface.isSealed()) {
      throw new IllegalArgumentException("Interface must be sealed: " + sealedInterface);
    }

    Class<?>[] permittedSubclasses = sealedInterface.getPermittedSubclasses();
    Map<Class<?>, TypeStructureAST> hierarchyStructures = new LinkedHashMap<>();

    for (Class<?> permittedClass : permittedSubclasses) {
      TypeStructureAST structure = analyze(permittedClass);
      hierarchyStructures.put(permittedClass, structure);

      // Recursively analyze nested sealed interfaces
      if (permittedClass.isSealed()) {
        hierarchyStructures.putAll(analyzeSealedHierarchy(permittedClass));
      }
    }

    return hierarchyStructures;
  }

  /**
   * Validates that a type structure is suitable for serialization.
   * This performs static verification to catch unsupported patterns at construction time.
   *
   * @param ast The AST to validate
   * @throws IllegalArgumentException if the structure contains unsupported patterns
   */
  public static void validateSerializationSupport(TypeStructureAST ast) {
    Objects.requireNonNull(ast, "AST cannot be null");

    if (ast.isEmpty()) {
      throw new IllegalArgumentException("Empty AST is not valid for serialization");
    }

    // Validate that the structure terminates with a leaf node
    TagWithType leaf = ast.leaf();
    if (!leaf.isLeaf()) {
      throw new IllegalArgumentException("AST must terminate with a leaf node (primitive or user-defined): " + leaf);
    }

    // Validate map structures have proper separator placement
    List<TagWithType> tags = ast.tagTypes();
    boolean inMapContext = false;
    boolean foundSeparator = false;

    for (int i = 0; i < tags.size(); i++) {
      TagWithType current = tags.get(i);

      if (current.tag() == StructuralTag.MAP) {
        inMapContext = true;
        foundSeparator = false;
      } else if (current.tag() == StructuralTag.MAP_SEPARATOR) {
        if (!inMapContext) {
          throw new IllegalArgumentException("MAP_SEPARATOR found outside of map context at position " + i);
        }
        if (foundSeparator) {
          throw new IllegalArgumentException("Multiple MAP_SEPARATOR found in single map at position " + i);
        }
        foundSeparator = true;
      } else if (current.isLeaf() && inMapContext && !foundSeparator) {
        // We've reached a leaf in map context but haven't found separator yet
        // This means we're processing the key type - continue
      } else if (current.isLeaf() && inMapContext && foundSeparator) {
        // We've reached a leaf after separator - map is complete
        inMapContext = false;
      }
    }

    // If we ended in map context without finding separator, that's an error
    if (inMapContext && !foundSeparator) {
      throw new IllegalArgumentException("Map structure is incomplete - missing MAP_SEPARATOR");
    }
  }

  /**
   * Clears the analysis cache. Useful for testing or memory management.
   */
  public static void clearCache() {
    TYPE_CACHE.clear();
  }

  /**
   * Returns the current size of the analysis cache.
   */
  public static int getCacheSize() {
    return TYPE_CACHE.size();
  }
}
