package io.github.simbo1905.no.framework.ast;

import java.lang.reflect.Type;
import java.util.List;

/**
 * Abstract Syntax Tree (AST) construction for Java generic type hierarchies.
 * <p>
 * This package implements recursive descent parsing of Java's Type system,
 * constructing an AST representation that enables compile-time generation
 * of specialized serialization code through multi-stage programming.
 * <p>
 * The AST preserves both structural information (container hierarchy) and
 * semantic information (concrete Java classes) to enable type-safe code generation.
 * <p>
 * Structural tags representing the container hierarchy of generic types.
 * These tags form the backbone of the AST, representing operations that
 * must be performed during serialization/deserialization.
 */
public enum StructuralTag {
  // Container tags - require recursive processing
  ARRAY,
  LIST,
  OPTIONAL,
  MAP,

  // Primitive tags - leaf nodes in the AST
  BOOLEAN,
  BYTE,
  SHORT,
  CHARACTER,
  INTEGER,
  LONG,
  FLOAT,
  DOUBLE,
  STRING,
  UUID,

  // User-defined tags - leaf nodes requiring special handling
  ENUM,
  RECORD,
  INTERFACE,

  // Special marker for map key/value separation
  MAP_SEPARATOR
}

/**
 * Immutable pair representing a single node in the type structure AST.
 * Each TagWithType represents both the structural operation (tag) and
 * the concrete type information needed for runtime dispatch.
 *
 * @param tag The structural tag indicating the operation type
 * @param type The concrete Java type for runtime dispatch
 */
record TagWithType(StructuralTag tag, Type type) {

  /**
   * Creates a TagWithType for container types that require recursive processing.
   */
  public static TagWithType container(StructuralTag containerTag, Type containerType) {
    if (!isContainerTag(containerTag)) {
      throw new IllegalArgumentException("Expected container tag, got: " + containerTag);
    }
    return new TagWithType(containerTag, containerType);
  }

  /**
   * Creates a TagWithType for primitive leaf types.
   */
  public static TagWithType primitive(StructuralTag primitiveTag, Type primitiveType) {
    if (!isPrimitiveTag(primitiveTag)) {
      throw new IllegalArgumentException("Expected primitive tag, got: " + primitiveTag);
    }
    return new TagWithType(primitiveTag, primitiveType);
  }

  /**
   * Creates a TagWithType for user-defined types (records, enums, interfaces).
   */
  public static TagWithType userDefined(StructuralTag userTag, Type userType) {
    if (!isUserDefinedTag(userTag)) {
      throw new IllegalArgumentException("Expected user-defined tag, got: " + userTag);
    }
    return new TagWithType(userTag, userType);
  }

  /**
   * Creates a special MAP_SEPARATOR marker for separating key and value types in maps.
   */
  public static TagWithType mapSeparator() {
    return new TagWithType(StructuralTag.MAP_SEPARATOR, null);
  }

  static boolean isContainerTag(StructuralTag tag) {
    return tag == StructuralTag.ARRAY || tag == StructuralTag.LIST ||
        tag == StructuralTag.OPTIONAL || tag == StructuralTag.MAP;
  }

  static boolean isPrimitiveTag(StructuralTag tag) {
    return tag == StructuralTag.BOOLEAN || tag == StructuralTag.BYTE ||
        tag == StructuralTag.SHORT || tag == StructuralTag.CHARACTER ||
        tag == StructuralTag.INTEGER || tag == StructuralTag.LONG ||
        tag == StructuralTag.FLOAT || tag == StructuralTag.DOUBLE ||
        tag == StructuralTag.STRING || tag == StructuralTag.UUID;
  }

  static boolean isUserDefinedTag(StructuralTag tag) {
    return tag == StructuralTag.ENUM || tag == StructuralTag.RECORD ||
        tag == StructuralTag.INTERFACE;
  }

  /**
   * Returns true if this tag represents a container that requires recursive processing.
   */
  public boolean isContainer() {
    return isContainerTag(tag);
  }

  /**
   * Returns true if this tag represents a primitive leaf type.
   */
  public boolean isPrimitive() {
    return isPrimitiveTag(tag);
  }

  /**
   * Returns true if this tag represents a user-defined type.
   */
  public boolean isUserDefined() {
    return isUserDefinedTag(tag);
  }

  /**
   * Returns true if this tag represents a leaf node in the AST (primitive or user-defined).
   */
  public boolean isLeaf() {
    return isPrimitive() || isUserDefined();
  }
}

/**
 * Complete Abstract Syntax Tree representation of a Java generic type hierarchy.
 * <p>
 * This record implements the result of recursive descent parsing of Java's Type system.
 * The tagTypes list represents a flattened, left-to-right traversal of the type structure
 * that preserves both the container hierarchy and the semantic type information.
 * <p>
 * The AST enables compile-time generation of specialized serialization code through
 * multi-stage programming, where each container operation delegates to its component
 * handlers in a right-to-left construction pattern.
 * <p>
 * Example: List&lt;Optional&lt;String[]&gt;&gt; produces:
 * tagTypes: [LIST, OPTIONAL, ARRAY, STRING] with corresponding Type objects
 *
 * @param tagTypes Immutable list representing the complete type structure AST
 */
record TypeStructureAST(List<TagWithType> tagTypes) {

  /**
   * Constructs a TypeStructureAST with defensive copying to ensure immutability.
   */
  public TypeStructureAST(List<TagWithType> tagTypes) {
    this.tagTypes = List.copyOf(tagTypes);
  }

  /**
   * Returns the number of nodes in the AST.
   */
  public int size() {
    return tagTypes.size();
  }

  /**
   * Returns true if the AST is empty.
   */
  public boolean isEmpty() {
    return tagTypes.isEmpty();
  }

  /**
   * Gets the TagWithType at the specified position.
   */
  public TagWithType get(int index) {
    return tagTypes.get(index);
  }

  /**
   * Returns the root (first) tag in the type structure.
   * This represents the outermost container or the primitive type itself.
   */
  public TagWithType root() {
    if (isEmpty()) {
      throw new IllegalStateException("Cannot get root of empty AST");
    }
    return tagTypes.getFirst();
  }

  /**
   * Returns the leaf (last) tag in the type structure.
   * This represents the innermost primitive or user-defined type.
   */
  public TagWithType leaf() {
    if (isEmpty()) {
      throw new IllegalStateException("Cannot get leaf of empty AST");
    }
    return tagTypes.getLast();
  }

  /**
   * Returns a substructure starting from the given index.
   * This is useful for recursive processing of nested structures.
   */
  public TypeStructureAST substructure(int fromIndex) {
    if (fromIndex < 0 || fromIndex >= tagTypes.size()) {
      throw new IndexOutOfBoundsException("Index: " + fromIndex + ", Size: " + tagTypes.size());
    }
    return new TypeStructureAST(tagTypes.subList(fromIndex, tagTypes.size()));
  }

  /**
   * Returns true if this AST represents a simple (non-nested) type.
   * Simple types have only one tag and are either primitives or user-defined types.
   */
  public boolean isSimple() {
    return tagTypes.size() == 1 && tagTypes.getFirst().isLeaf();
  }

  /**
   * Returns true if this AST represents a nested structure with containers.
   */
  public boolean isNested() {
    return tagTypes.size() > 1 || (tagTypes.size() == 1 && tagTypes.getFirst().isContainer());
  }

  /**
   * Counts the number of container operations in this AST.
   */
  public long containerCount() {
    return tagTypes.stream().filter(TagWithType::isContainer).count();
  }

  /**
   * Returns a string representation showing the structure hierarchy.
   * Example: "LIST<OPTIONAL<ARRAY<STRING>>>"
   */
  public String toStructureString() {
    if (isEmpty()) {
      return "<empty>";
    }

    StringBuilder sb = new StringBuilder();
    int openBrackets = 0;

    for (TagWithType tagType : tagTypes) {
      if (tagType.tag() == StructuralTag.MAP_SEPARATOR) {
        sb.append(", ");
        continue;
      }

      sb.append(tagType.tag().name());

      if (tagType.isContainer()) {
        sb.append("<");
        openBrackets++;
      }
    }

    // Close all open brackets
    sb.append(">".repeat(openBrackets));

    return sb.toString();
  }

  @Override
  public String toString() {
    return "TypeStructureAST{" + toStructureString() + "}";
  }
}
