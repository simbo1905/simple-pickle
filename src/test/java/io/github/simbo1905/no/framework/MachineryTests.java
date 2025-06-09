package io.github.simbo1905.no.framework;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static io.github.simbo1905.no.framework.Pickler.LOGGER;

/// Package-private tests for core machinery components
/// Tests the internal implementation details that are not part of the public API
@SuppressWarnings("auxiliaryclass")
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

  // Map test records
  record SimpleMapTest(Map<String, Integer> map) {}
  record NestedMapTest(Map<Long, Optional<String>> map) {}
  record ComplexMapTest(Map<UUID, List<String>> map) {}

  @Test
  void testMapTypeAnalysis() {
    var recordClass = SimpleMapTest.class;
    var components = recordClass.getRecordComponents();
    
    // Test Map<String, Integer>
    var mapType = components[0].getGenericType();
    var result = TypeStructure.analyze(mapType);
    
    LOGGER.fine(() -> "Map<String, Integer> analysis:");
    LOGGER.fine(() -> "  TagTypes: " + result.tagTypes());
    
    // Should be: [MAP, STRING, INTEGER]
    assertThat(result.tagTypes()).hasSize(3);
    assertThat(result.tagTypes().get(0).tag()).isEqualTo(Tag.MAP);
    assertThat(result.tagTypes().get(0).type()).isEqualTo(Map.class);
    assertThat(result.tagTypes().get(1).tag()).isEqualTo(Tag.STRING);
    assertThat(result.tagTypes().get(1).type()).isEqualTo(String.class);
    assertThat(result.tagTypes().get(2).tag()).isEqualTo(Tag.INTEGER);
    assertThat(result.tagTypes().get(2).type()).isEqualTo(Integer.class);
  }

  @Test
  void testNestedMapTypeAnalysis() {
    var recordClass = NestedMapTest.class;
    var components = recordClass.getRecordComponents();
    
    // Test Map<Long, Optional<String>>
    var mapType = components[0].getGenericType();
    var result = TypeStructure.analyze(mapType);
    
    LOGGER.fine(() -> "Map<Long, Optional<String>> analysis:");
    LOGGER.fine(() -> "  TagTypes: " + result.tagTypes());
    
    // Should be: [MAP, LONG, OPTIONAL, STRING]
    assertThat(result.tagTypes()).hasSize(4);
    assertThat(result.tagTypes().get(0).tag()).isEqualTo(Tag.MAP);
    assertThat(result.tagTypes().get(0).type()).isEqualTo(Map.class);
    assertThat(result.tagTypes().get(1).tag()).isEqualTo(Tag.LONG);
    assertThat(result.tagTypes().get(1).type()).isEqualTo(Long.class);
    assertThat(result.tagTypes().get(2).tag()).isEqualTo(Tag.OPTIONAL);
    assertThat(result.tagTypes().get(2).type()).isEqualTo(Optional.class);
    assertThat(result.tagTypes().get(3).tag()).isEqualTo(Tag.STRING);
    assertThat(result.tagTypes().get(3).type()).isEqualTo(String.class);
  }

  @Test
  void testComplexMapTypeAnalysis() {
    var recordClass = ComplexMapTest.class;
    var components = recordClass.getRecordComponents();
    
    // Test Map<UUID, List<String>>
    var mapType = components[0].getGenericType();
    var result = TypeStructure.analyze(mapType);
    
    LOGGER.fine(() -> "Map<UUID, List<String>> analysis:");
    LOGGER.fine(() -> "  TagTypes: " + result.tagTypes());
    
    // Should be: [MAP, UUID, LIST, STRING]
    assertThat(result.tagTypes()).hasSize(4);
    assertThat(result.tagTypes().get(0).tag()).isEqualTo(Tag.MAP);
    assertThat(result.tagTypes().get(0).type()).isEqualTo(Map.class);
    assertThat(result.tagTypes().get(1).tag()).isEqualTo(Tag.UUID);
    assertThat(result.tagTypes().get(1).type()).isEqualTo(UUID.class);
    assertThat(result.tagTypes().get(2).tag()).isEqualTo(Tag.LIST);
    assertThat(result.tagTypes().get(2).type()).isEqualTo(List.class);
    assertThat(result.tagTypes().get(3).tag()).isEqualTo(Tag.STRING);
    assertThat(result.tagTypes().get(3).type()).isEqualTo(String.class);
  }

  // Test sealed interface hierarchy analysis for INTERFACE tag detection
  
  // Case 1: Sealed interface with only records
  public sealed interface RecordOnlyInterface permits RecordImpl1, RecordImpl2 {}
  public record RecordImpl1(String name) implements RecordOnlyInterface {}
  public record RecordImpl2(int value) implements RecordOnlyInterface {}
  
  // Case 2: Sealed interface with only enums
  public sealed interface EnumOnlyInterface permits EnumImpl1, EnumImpl2 {}
  public enum EnumImpl1 implements EnumOnlyInterface { A, B }
  public enum EnumImpl2 implements EnumOnlyInterface { X, Y }
  
  // Case 3: Sealed interface with both records and enums (should get INTERFACE tag)
  public sealed interface MixedInterface permits MixedRecord, MixedEnum {}
  public record MixedRecord(String data) implements MixedInterface {}
  public enum MixedEnum implements MixedInterface { ONE, TWO }
  
  // Case 4: Deeply nested sealed interfaces
  public sealed interface OuterInterface permits InnerInterface, OuterRecord {}
  public sealed interface InnerInterface extends OuterInterface permits InnerRecord, InnerEnum {}
  public record OuterRecord(String outer) implements OuterInterface {}
  public record InnerRecord(String inner) implements InnerInterface {}
  public enum InnerEnum implements InnerInterface { NESTED }
  
  @Test
  void testSealedInterfaceTagAnalysis() {
    // Case 1: Interface with only records should get RECORD tag
    Tag tag1 = Tag.fromClass(RecordOnlyInterface.class);
    assertThat(tag1).isEqualTo(Tag.RECORD);
    
    // Case 2: Interface with only enums should get ENUM tag
    Tag tag2 = Tag.fromClass(EnumOnlyInterface.class);
    assertThat(tag2).isEqualTo(Tag.ENUM);
    
    // Case 3: Interface with both should get INTERFACE tag
    Tag tag3 = Tag.fromClass(MixedInterface.class);
    assertThat(tag3).isEqualTo(Tag.INTERFACE);
    
    // Case 4: Deeply nested with mixed types should get INTERFACE tag
    Tag tag4 = Tag.fromClass(OuterInterface.class);
    assertThat(tag4).isEqualTo(Tag.INTERFACE);
    
    // Also test the inner interface
    Tag tag5 = Tag.fromClass(InnerInterface.class);
    assertThat(tag5).isEqualTo(Tag.INTERFACE);
  }
  
  // Test TypeStructure analysis with INTERFACE tag
  public record InterfaceArrayRecord(MixedInterface[] mixedArray) {}
  public record InterfaceListRecord(List<MixedInterface> mixedList) {}
  public record InterfaceOptionalRecord(Optional<MixedInterface> mixedOptional) {}
  
  @Test
  void testTypeStructureWithInterfaceTag() {
    // Test MixedInterface[] 
    var recordClass = InterfaceArrayRecord.class;
    var components = recordClass.getRecordComponents();
    var arrayType = components[0].getGenericType();
    var result = TypeStructure.analyze(arrayType);
    
    // Should be: [ARRAY, INTERFACE]
    assertThat(result.tagTypes()).hasSize(2);
    assertThat(result.tagTypes().get(0).tag()).isEqualTo(Tag.ARRAY);
    assertThat(result.tagTypes().get(1).tag()).isEqualTo(Tag.INTERFACE);
    assertThat(result.tagTypes().get(1).type()).isEqualTo(MixedInterface.class);
  }
}
