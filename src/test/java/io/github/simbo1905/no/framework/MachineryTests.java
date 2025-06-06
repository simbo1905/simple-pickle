package io.github.simbo1905.no.framework;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

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
    final var pickler = Pickler.of(ArrayRecord.class);
    final var impl = (PicklerImpl<ArrayRecord>) pickler;
    final var discoveredClasses = impl.userTypes;
    
    // Array type discovery test
    assertThat(discoveredClasses).hasSize(2).contains(ArrayRecord.class, NestedRecord.class);
  }

  @Test
  void testListTypeDiscovery() {
    final var pickler = Pickler.of(ListRecord.class);
    final var impl = (PicklerImpl<ListRecord>) pickler;
    final var discoveredClasses = impl.userTypes;
    
    // List type discovery test
    assertThat(discoveredClasses).hasSize(2).contains(ListRecord.class, NestedRecord.class);
  }

  @Test
  void testOptionalTypeDiscovery() {
    final var pickler = Pickler.of(OptionalRecord.class);
    final var impl = (PicklerImpl<OptionalRecord>) pickler;
    final var discoveredClasses = impl.userTypes;
    
    // Optional type discovery test
    assertThat(discoveredClasses).hasSize(2).contains(OptionalRecord.class, NestedRecord.class);
  }

  @Test
  void testDirectTypeDiscovery() {
    final var pickler = Pickler.of(DirectRecord.class);
    final var impl = (PicklerImpl<DirectRecord>) pickler;
    final var discoveredClasses = impl.userTypes;
    
    // Direct type discovery test
    assertThat(discoveredClasses).hasSize(2).contains(DirectRecord.class, NestedRecord.class);
  }
}
