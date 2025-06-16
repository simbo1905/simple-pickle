package io.github.simbo1905.no.framework;

import java.lang.reflect.*;

/// Public sealed interface for the Type Expression protocol
/// All type expression nodes are nested within this interface to provide a clean API
sealed interface TypeExpr permits
    TypeExpr.ArrayNode, TypeExpr.ListNode, TypeExpr.OptionalNode, TypeExpr.MapNode, TypeExpr.PrimitiveNode {

  /// Recursive descent parser for Java types - builds tree bottom-up
  static TypeExpr analyzeType(Type type) {
      // Handle arrays first (both primitive arrays and object arrays)
      if (type instanceof Class<?> clazz && clazz.isArray()) {
          TypeExpr elementTypeExpr = analyzeType(clazz.getComponentType());
          return new ArrayNode(elementTypeExpr);
      }

      // Handle generic array types (e.g., T[] where T is a type parameter)
      if (type instanceof GenericArrayType genericArrayType) {
          TypeExpr elementTypeExpr = analyzeType(genericArrayType.getGenericComponentType());
          return new ArrayNode(elementTypeExpr);
      }

      // Handle parameterized types (List<T>, Map<K,V>, Optional<T>)
      if (type instanceof ParameterizedType paramType) {
          Type rawType = paramType.getRawType();

          if (rawType instanceof Class<?> rawClass) {
              Type[] typeArgs = paramType.getActualTypeArguments();

              // Handle List<T>
              if (java.util.List.class.isAssignableFrom(rawClass)) {
                  if (typeArgs.length == 1) {
                      TypeExpr elementTypeExpr = analyzeType(typeArgs[0]);
                      return new ListNode(elementTypeExpr);
                  } else {
                      throw new IllegalArgumentException("List must have exactly one type argument: " + type);
                  }
              }

              // Handle Optional<T>
              if (java.util.Optional.class.isAssignableFrom(rawClass)) {
                  if (typeArgs.length == 1) {
                      TypeExpr wrappedTypeExpr = analyzeType(typeArgs[0]);
                      return new OptionalNode(wrappedTypeExpr);
                  } else {
                      throw new IllegalArgumentException("Optional must have exactly one type argument: " + type);
                  }
              }

              // Handle Map<K,V>
              if (java.util.Map.class.isAssignableFrom(rawClass)) {
                  if (typeArgs.length == 2) {
                      TypeExpr keyTypeExpr = analyzeType(typeArgs[0]);
                      TypeExpr valueTypeExpr = analyzeType(typeArgs[1]);
                      return new MapNode(keyTypeExpr, valueTypeExpr);
                  } else {
                      throw new IllegalArgumentException("Map must have exactly two type arguments: " + type);
                  }
              }
          }
      }

      // Handle raw classes (primitive types, records, enums, interfaces)
      if (type instanceof Class<?> clazz) {
          PrimitiveType primType = classifyClass(clazz);
          return new PrimitiveNode(primType, clazz);
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

  /// Classifies a Java Class into the appropriate PrimitiveType
  static PrimitiveType classifyClass(Class<?> clazz) {
      // Handle primitive types and their boxed equivalents
      if (clazz == boolean.class || clazz == Boolean.class) {
          return PrimitiveType.BOOLEAN;
      }
      if (clazz == byte.class || clazz == Byte.class) {
          return PrimitiveType.BYTE;
      }
      if (clazz == short.class || clazz == Short.class) {
          return PrimitiveType.SHORT;
      }
      if (clazz == char.class || clazz == Character.class) {
          return PrimitiveType.CHARACTER;
      }
      if (clazz == int.class || clazz == Integer.class) {
          return PrimitiveType.INTEGER;
      }
      if (clazz == long.class || clazz == Long.class) {
          return PrimitiveType.LONG;
      }
      if (clazz == float.class || clazz == Float.class) {
          return PrimitiveType.FLOAT;
      }
      if (clazz == double.class || clazz == Double.class) {
          return PrimitiveType.DOUBLE;
      }

      // Handle reference types
      if (clazz == String.class) {
          return PrimitiveType.STRING;
      }
      if (clazz == java.util.UUID.class) {
          return PrimitiveType.UUID;
      }

      // Handle user-defined types
      if (clazz.isEnum()) {
          return PrimitiveType.ENUM;
      }
      if (clazz.isRecord()) {
          return PrimitiveType.RECORD;
      }
      if (clazz.isInterface() || clazz.isSealed()) {
          return PrimitiveType.INTERFACE;
      }

      throw new IllegalArgumentException("Unsupported class type: " + clazz);
  }

  /// Helper method to get a string representation for debugging
    /// Example: LIST(STRING) or MAP(STRING, INTEGER)
    default String toTreeString() {
        return switch (this) {
            case ArrayNode(var element) -> "ARRAY(" + element.toTreeString() + ")";
            case ListNode(var element) -> "LIST(" + element.toTreeString() + ")";
            case OptionalNode(var wrapped) -> "OPTIONAL(" + wrapped.toTreeString() + ")";
            case MapNode(var key, var value) -> "MAP(" + key.toTreeString() + ", " + value.toTreeString() + ")";
            case PrimitiveNode(var type, var javaType) -> type.name();
        };
    }
    
    /// Static method to analyze a type and return the TypeExpr
    /// This is the public API entry point that delegates to package-private implementation
    static TypeExpr analyze(Type type) {
        java.util.Objects.requireNonNull(type, "Type cannot be null");
        return analyzeType(type);
    }
    
    /// Container node for arrays - has one child (element type)
    record ArrayNode(TypeExpr element) implements TypeExpr {
        public ArrayNode {
            java.util.Objects.requireNonNull(element, "Array element type cannot be null");
        }
    }
    
    /// Container node for lists - has one child (element type)
    record ListNode(TypeExpr element) implements TypeExpr {
        public ListNode {
            java.util.Objects.requireNonNull(element, "List element type cannot be null");
        }
    }
    
    /// Container node for optionals - has one child (wrapped type)
    record OptionalNode(TypeExpr wrapped) implements TypeExpr {
        public OptionalNode {
            java.util.Objects.requireNonNull(wrapped, "Optional wrapped type cannot be null");
        }
    }
    
    /// Container node for maps - has two children (key type, value type)
    record MapNode(TypeExpr key, TypeExpr value) implements TypeExpr {
        public MapNode {
            java.util.Objects.requireNonNull(key, "Map key type cannot be null");
            java.util.Objects.requireNonNull(value, "Map value type cannot be null");
        }
    }
    
    /// Leaf node for all primitive/value types
    /// Stores both the category (enum) and the actual Java type
    record PrimitiveNode(PrimitiveType type, Type javaType) implements TypeExpr {
        public PrimitiveNode {
            java.util.Objects.requireNonNull(type, "Primitive type cannot be null");
            java.util.Objects.requireNonNull(javaType, "Java type cannot be null");
        }
        
        /// Override to only show the type name, not the Java type
        @Override
        public String toTreeString() {
            return type.name();
        }
    }
    
    /// Enum for all value-like types (leaf nodes in the TypeExpr)
    enum PrimitiveType {
        BOOLEAN, BYTE, SHORT, CHARACTER, INTEGER, LONG, FLOAT, DOUBLE,
        STRING, UUID, ENUM, RECORD, INTERFACE
    }
}
