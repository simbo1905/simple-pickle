package io.github.simbo1905.no.framework;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/// Package-private tests for core machinery components
/// Tests the internal implementation details that are not part of the public API
class MachineryTests {

  @BeforeAll
  static void setupLogging() {
    io.github.simbo1905.LoggingControl.setupCleanLogging();
  }

  /// Nested record to test discovery
  public record NestedRecord(String name) {}
  
  /// Comprehensive test record with all field types to verify baseline discovery
  public record ComprehensiveTestRecord(
      // Primitives (should NOT be discovered as user types)
      int primitiveInt,
      boolean primitiveBoolean,
      
      // Boxed primitives (built-in types, should NOT be discovered as user types)
      Integer boxedInt,
      Boolean boxedBoolean,
      
      // Built-in reference types (should NOT be discovered as user types)
      String string,
      java.util.UUID uuid,
      
      // Optional of built-in (should NOT discover the wrapped type as user type)
      java.util.Optional<String> optionalString,
      java.util.Optional<Integer> optionalInt,
      
      // Primitive arrays (should NOT be discovered as user types)
      byte[] byteArray,
      int[] intArray,
      
      // Built-in reference arrays (should NOT be discovered as user types)  
      String[] stringArray,
      java.util.UUID[] uuidArray,
      
      // User type (SHOULD be discovered)
      NestedRecord nested,
      
      // User type array (SHOULD discover component type)
      NestedRecord[] nestedArray
  ) {}

  /// Test record with same-package array - THIS WORKS
  public record SamePackageArrayRecord(NestedRecord[] nestedArray) {}
  
  /// Test records for all generic container types that should discover component types
  public record ArrayRecord(NestedRecord[] array) {}
  public record ListRecord(java.util.List<NestedRecord> list) {}  
  public record OptionalRecord(java.util.Optional<NestedRecord> optional) {}
  public record DirectRecord(NestedRecord direct) {} // Control - this should work

  @Test
  void testArrayTypeDiscovery() {
    final var pickler = Pickler.forClass(ArrayRecord.class);
    final var impl = (PicklerImpl<ArrayRecord>) pickler;
    final var discoveredClasses = impl.userTypes;
    
    // Array type discovery test
    assertThat(discoveredClasses).hasSize(2).contains(ArrayRecord.class, NestedRecord.class);
  }

  @Test
  void testListTypeDiscovery() {
    final var pickler = Pickler.forClass(ListRecord.class);
    final var impl = (PicklerImpl<ListRecord>) pickler;
    final var discoveredClasses = impl.userTypes;
    
    // List type discovery test
    assertThat(discoveredClasses).hasSize(2).contains(ListRecord.class, NestedRecord.class);
  }

  @Test
  void testOptionalTypeDiscovery() {
    final var pickler = Pickler.forClass(OptionalRecord.class);
    final var impl = (PicklerImpl<OptionalRecord>) pickler;
    final var discoveredClasses = impl.userTypes;
    
    // Optional type discovery test
    assertThat(discoveredClasses).hasSize(2).contains(OptionalRecord.class, NestedRecord.class);
  }

  @Test
  void testDirectTypeDiscovery() {
    final var pickler = Pickler.forClass(DirectRecord.class);
    final var impl = (PicklerImpl<DirectRecord>) pickler;
    final var discoveredClasses = impl.userTypes;
    
    // Direct type discovery test
    assertThat(discoveredClasses).hasSize(2).contains(DirectRecord.class, NestedRecord.class);
  }

  // Test record for TypeStructure analysis
  public record OptionalArrayTestRecord(
      java.util.Optional<String>[] stringOptionals,
      java.util.Optional<Integer>[] intOptionals
  ) {}

  @Test
  void testTypeStructureAnalysisForOptionalArrays() {
    // Get the generic type from record component - this retains generic info
    var recordClass = OptionalArrayTestRecord.class;
    var components = recordClass.getRecordComponents();
    
    // Test Optional<String>[] field
    var stringOptionalArrayType = components[0].getGenericType();
    var result = TypeStructure.analyze(stringOptionalArrayType);
    
    assertThat(result.tagTypes()).hasSize(3);
    // Should be: [ARRAY, OPTIONAL, STRING] with [Arrays.class, Optional.class, String.class]
    assertThat(result.tagTypes().get(0).tag()).isEqualTo(Tag.ARRAY);
    assertThat(result.tagTypes().get(0).type()).isEqualTo(Arrays.class);
    assertThat(result.tagTypes().get(1).tag()).isEqualTo(Tag.OPTIONAL);
    assertThat(result.tagTypes().get(1).type()).isEqualTo(Optional.class);
    assertThat(result.tagTypes().get(2).tag()).isEqualTo(Tag.STRING);
    assertThat(result.tagTypes().get(2).type()).isEqualTo(String.class);
  }

  // Test record for Optional of Array - the flipped case
  public record OptionalOfArrayTestRecord(
      java.util.Optional<String[]> optionalStringArray,
      java.util.Optional<Integer[]> optionalIntArray
  ) {}

  @Test
  void testTypeStructureAnalysisForOptionalOfArray() {
    // Test the flipped case - Optional<String[]> instead of Optional<String>[]
    var recordClass = OptionalOfArrayTestRecord.class;
    var components = recordClass.getRecordComponents();
    
    // Test Optional<String[]> field
    var optionalStringArrayType = components[0].getGenericType();
    var result = TypeStructure.analyze(optionalStringArrayType);
    
    assertThat(result.tagTypes()).hasSize(3);
    // Should be: [OPTIONAL, ARRAY, STRING] with [Optional.class, Arrays.class, String.class]
    assertThat(result.tagTypes().get(0).tag()).isEqualTo(Tag.OPTIONAL);
    assertThat(result.tagTypes().get(0).type()).isEqualTo(Optional.class);
    assertThat(result.tagTypes().get(1).tag()).isEqualTo(Tag.ARRAY);
    assertThat(result.tagTypes().get(1).type()).isEqualTo(Arrays.class);
    assertThat(result.tagTypes().get(2).tag()).isEqualTo(Tag.STRING);
    assertThat(result.tagTypes().get(2).type()).isEqualTo(String.class);
  }
}
