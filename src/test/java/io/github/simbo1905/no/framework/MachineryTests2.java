// SPDX-FileCopyrightText: 2025 Simon Massey
// SPDX-License-Identifier: MIT

package io.github.simbo1905.no.framework;

import io.github.simbo1905.no.framework.model.Person;
import io.github.simbo1905.no.framework.model.Simple;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.logging.ConsoleHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import static io.github.simbo1905.no.framework.Pickler.LOGGER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;

/// Test suite for the new unified pickler architecture
class MachineryTests2 {

    @BeforeAll
    static void setupLogging() {
        String logLevel = System.getProperty("java.util.logging.ConsoleHandler.level");
        if (logLevel != null) {
            System.out.println("MachineryTests2 logging initialized at level: " + logLevel);
            Level level = Level.parse(logLevel);
            Logger rootLogger = Logger.getLogger("");
            ConsoleHandler consoleHandler = new ConsoleHandler();
            consoleHandler.setLevel(level);
            rootLogger.addHandler(consoleHandler);
            rootLogger.setLevel(level);
        }
    }

    /// Test simple enum for Machinery2 usage
    public enum TestEnum {
        VALUE_A, VALUE_B, VALUE_C
    }

    /// Test simple record for Machinery2 usage
    public record TestRecord(String name, int value) {}

    /// Test record with enum component
    public record RecordWithEnum(TestEnum enumValue, String description) {}
    
    // Foundation test records - extracted from test methods to ensure public visibility
    
    /// Simple record with built-in types
    public record SimpleRecord(int value, String name) {}
    
    /// Complex record with collections and optionals 
    public record ComplexRecord(
        int[] numbers,
        List<String> texts,
        Optional<Integer> maybeValue,
        Map<String, Integer> mapping
    ) {}
    
    /// Nested record hierarchy for testing discovery
    public record InnerRecord(String text) {}
    public record OuterRecord(InnerRecord inner, int value) {}
    
    /// Ordinal mapping test records
    public record BRecord(String b) {}
    public record ZRecord(int z) {}
    public record NestedRecord(BRecord b, ZRecord z) {}
    
    /// Enum testing
    @SuppressWarnings("unused")
    public enum TestStatus { ACTIVE, INACTIVE }
    public record EnumRecord(TestStatus status) {}
    
    /// Built-in types testing
    public record SimpleWithBuiltIns(int number, String text, boolean flag) {}
    
    /// Comprehensive built-in types for Layer 2 testing
    public record AllBuiltInTypes(
        boolean flag,
        int number, 
        long bigNumber,
        String text,
        Optional<String> maybeText,
        List<Integer> numbers,
        Map<String, Integer> mapping,
        int[] primitiveArray,
        String[] objectArray
    ) {}
    
    /// Unique nested records for specific tests (avoiding name conflicts)
    public record Inner(String value) {}
    public record Outer(Inner inner, int count) {}
    
    /// Container types testing
    public record WithContainers(int[] numbers, List<String> texts, Map<String, Integer> mapping) {}
    
    /// Layer 2 testing records
    public record ArrayTest(int[] numbers, String[] texts, boolean[] flags) {}
    public record CollectionTest(List<String> texts, Map<String, Integer> mapping) {}
    
    /// Layer 3 testing records - nested user types
    public record NestedUserTypes(InnerRecord inner, TestEnum status, String name) {}
    public record Layer3RecordWithEnum(TestEnum enumValue, int count) {}

    @Test
    void testUnifiedTypeDiscoveryForEnum() {
        // Test that unified type analysis works for a simple enum
        var analysis = Machinery2.analyzeAllReachableTypes(TestEnum.class);
        
        // Should discover exactly one type (the enum itself)
        assertThat(analysis.discoveredClasses()).hasSize(1);
        assertThat(analysis.discoveredClasses()[0]).isEqualTo(TestEnum.class);
        assertThat(analysis.tags()[0]).isEqualTo(Tag.ENUM);
        
        // Constructor and component accessors should be null for enums
        assertThat(analysis.constructors()[0]).isNull();
        assertThat(analysis.componentAccessors()[0]).isNull();
        
        // Should have ordinal mapping
        assertThat(analysis.getOrdinal(TestEnum.class)).isEqualTo(0);
    }

    @Test
    void testUnifiedTypeDiscoveryForSimpleRecord() {
        // Test that unified type analysis works for a simple record
        var analysis = Machinery2.analyzeAllReachableTypes(TestRecord.class);
        
        // Should discover exactly one type (the record itself)  
        assertThat(analysis.discoveredClasses()).hasSize(1);
        assertThat(analysis.discoveredClasses()[0]).isEqualTo(TestRecord.class);
        assertThat(analysis.tags()[0]).isEqualTo(Tag.RECORD);
        
        // Should have constructor and component accessors for records
        assertThat(analysis.constructors()[0]).isNotNull();
        assertThat(analysis.componentAccessors()[0]).isNotNull();
        assertThat(analysis.componentAccessors()[0]).hasSize(2); // name, value components
        
        // Should have ordinal mapping
        assertThat(analysis.getOrdinal(TestRecord.class)).isEqualTo(0);
    }

    @Test
    void testUnifiedTypeDiscoveryForRecordWithEnum() {
        // Test that unified type analysis discovers both record and enum
        var analysis = Machinery2.analyzeAllReachableTypes(RecordWithEnum.class);
        
        // Should discover both types, sorted lexicographically
        assertThat(analysis.discoveredClasses()).hasSize(2);
        
        // RecordWithEnum should come before TestEnum lexicographically
        String[] expectedOrder = {RecordWithEnum.class.getName(), TestEnum.class.getName()};
        String[] actualOrder = Arrays.stream(analysis.discoveredClasses())
            .map(Class::getName)
            .toArray(String[]::new);
        Arrays.sort(expectedOrder);
        Arrays.sort(actualOrder);
        assertThat(actualOrder).isEqualTo(expectedOrder);
        
        // Check that both types have correct tags
        for (int i = 0; i < analysis.discoveredClasses().length; i++) {
            Class<?> clazz = analysis.discoveredClasses()[i];
            Tag tag = analysis.tags()[i];
            
            if (clazz.equals(RecordWithEnum.class)) {
                assertThat(tag).isEqualTo(Tag.RECORD);
                assertThat(analysis.constructors()[i]).isNotNull();
                assertThat(analysis.componentAccessors()[i]).hasSize(2);
            } else if (clazz.equals(TestEnum.class)) {
                assertThat(tag).isEqualTo(Tag.ENUM);
                assertThat(analysis.constructors()[i]).isNull();
                assertThat(analysis.componentAccessors()[i]).isNull();
            }
        }
        
        // Should have ordinal mappings for both
        assertThat(analysis.classToOrdinal()).containsKey(RecordWithEnum.class);
        assertThat(analysis.classToOrdinal()).containsKey(TestEnum.class);
    }

    @Test
    void testLexicographicalOrdering() {
        // Test that classes are sorted lexicographically for stable ordinals
        var analysis = Machinery2.analyzeAllReachableTypes(RecordWithEnum.class);
        
        // Verify lexicographical ordering
        String[] classNames = Arrays.stream(analysis.discoveredClasses())
            .map(Class::getName)
            .toArray(String[]::new);
            
        String[] sortedNames = classNames.clone();
        Arrays.sort(sortedNames);
        
        assertThat(classNames).isEqualTo(sortedNames);
    }

    @Test  
    void testPicklerImplConstructorWorks() {
        // PicklerImpl constructor should now work with type analysis
        var pickler = new PicklerImpl<>(TestEnum.class);
        assertThat(pickler).isNotNull();
        
        // Test with record too
        var recordPickler = new PicklerImpl<>(TestRecord.class);
        assertThat(recordPickler).isNotNull();
        
        // Test with record containing enum
        var complexPickler = new PicklerImpl<>(RecordWithEnum.class);
        assertThat(complexPickler).isNotNull();
    }

    @Test
    void testUnifiedTypeAnalysisWithExistingModelClasses() {
        // Test with existing model classes to ensure compatibility
        var analysis = Machinery2.analyzeAllReachableTypes(Person.class);
        
        // Should discover Person and possibly Address
        assertThat(analysis.discoveredClasses().length).isGreaterThan(0);
        assertThat(Arrays.asList(analysis.discoveredClasses())).contains(Person.class);
        
        // All discovered classes should be records
        for (int i = 0; i < analysis.discoveredClasses().length; i++) {
            Class<?> clazz = analysis.discoveredClasses()[i];
            assertThat(clazz.isRecord()).isTrue();
            assertThat(analysis.tags()[i]).isEqualTo(Tag.RECORD);
        }
    }

    @Test
    void testUnifiedTypeAnalysisWithSimpleRecord() {
        // Test with Simple record from existing model
        var analysis = Machinery2.analyzeAllReachableTypes(Simple.class);
        
        // Should discover exactly Simple
        assertThat(analysis.discoveredClasses()).hasSize(1);
        assertThat(analysis.discoveredClasses()[0]).isEqualTo(Simple.class);
        assertThat(analysis.tags()[0]).isEqualTo(Tag.RECORD);
    }
    
    @Test
    void testMachinery2WithBuiltInTypes() {
        // Test that Machinery2 correctly handles records with built-in types
        var analysis = Machinery2.analyzeAllReachableTypes(SimpleWithBuiltIns.class);
        
        // Should discover only the record itself (built-ins aren't "user types")
        assertThat(analysis.discoveredClasses()).hasSize(1);
        assertThat(analysis.discoveredClasses()[0]).isEqualTo(SimpleWithBuiltIns.class);
        assertThat(analysis.tags()[0]).isEqualTo(Tag.RECORD);
        
        // Should have constructor and component accessors for the record
        assertThat(analysis.constructors()[0]).isNotNull();
        assertThat(analysis.componentAccessors()[0]).hasSize(3); // int, String, boolean
    }
    
    @Test
    void testMachinery2WithNestedRecords() {
        // Test that Machinery2 finds nested record types  
        var analysis = Machinery2.analyzeAllReachableTypes(Outer.class);
        
        // Should discover both Outer and Inner records
        assertThat(analysis.discoveredClasses()).hasSize(2);
        
        // Check lexicographical ordering
        var classNames = Arrays.stream(analysis.discoveredClasses())
            .map(Class::getSimpleName)
            .toArray(String[]::new);
        assertThat(classNames).containsExactly("Inner", "Outer"); // Alphabetical order
        
        // Both should be records with proper metadata
        for (int i = 0; i < analysis.discoveredClasses().length; i++) {
            assertThat(analysis.tags()[i]).isEqualTo(Tag.RECORD);
            assertThat(analysis.constructors()[i]).isNotNull();
            assertThat(analysis.componentAccessors()[i]).isNotNull();
        }
    }
    
    @Test
    void testMachinery2WithArraysAndLists() {
        // Test that Machinery2 handles container types without discovering their component types as user types
        var analysis = Machinery2.analyzeAllReachableTypes(WithContainers.class);
        
        // Should discover only the record itself (String/Integer are built-ins, not user types)
        assertThat(analysis.discoveredClasses()).hasSize(1);
        assertThat(analysis.discoveredClasses()[0]).isEqualTo(WithContainers.class);
        assertThat(analysis.tags()[0]).isEqualTo(Tag.RECORD);
    }
    
    @Test
    void testMachinery2OrdinalMappingConsistency() {
        // Test that ordinal mappings are consistent and work correctly
        var analysis = Machinery2.analyzeAllReachableTypes(RecordWithEnum.class);
        
        // Check that every discovered class has a consistent ordinal mapping
        for (int i = 0; i < analysis.discoveredClasses().length; i++) {
            Class<?> clazz = analysis.discoveredClasses()[i];
            int ordinal = analysis.getOrdinal(clazz);
            assertThat(ordinal).isEqualTo(i);
            assertThat(analysis.discoveredClasses()[ordinal]).isEqualTo(clazz);
        }
        
        // Test unknown class throws exception
        assertThrows(IllegalArgumentException.class, () -> analysis.getOrdinal(String.class));
    }
    
    @Test
    void testPicklerOfFactoryMethod() {
        // Test that Pickler.of() factory method works
        var enumPickler = Pickler.of(TestEnum.class);
        assertThat(enumPickler).isNotNull();
        assertThat(enumPickler).isInstanceOf(PicklerImpl.class);
        
        var recordPickler = Pickler.of(TestRecord.class);
        assertThat(recordPickler).isNotNull();
        assertThat(recordPickler).isInstanceOf(PicklerImpl.class);
    }
    
    @Test
    void testPicklerOfInvalidTypes() {
        // Test that Pickler.of() rejects invalid types
        assertThrows(IllegalArgumentException.class, () -> Pickler.of(String.class));
        assertThrows(IllegalArgumentException.class, () -> Pickler.of(Integer.class));
        assertThrows(NullPointerException.class, () -> Pickler.of(null));
    }
    
    @Test
    void testBasicEnumSerialization() throws Exception {
        // Test basic enum serialization with ordinals
        var pickler = Pickler.of(TestEnum.class);
        
        try (var writeBuffer = pickler.allocateForWriting(64)) {
            int bytesWritten = pickler.serialize(writeBuffer, TestEnum.VALUE_A);
            assertThat(bytesWritten).isEqualTo(1); // Should write exactly 1 byte for ordinal 0
        }
    }
    
    @Test 
    void testNullSerialization() throws Exception {
        // Test null handling
        var pickler = Pickler.of(TestEnum.class);
        
        try (var writeBuffer = pickler.allocateForWriting(64)) {
            int bytesWritten = pickler.serialize(writeBuffer, null);
            assertThat(bytesWritten).isEqualTo(1); // Should write exactly 1 byte for NULL marker (0)
        }
    }
    
    @Test
    void testBasicRecordSerialization() throws Exception {
        // Test basic record serialization with built-in types
        var pickler = Pickler.of(SimpleRecord.class);
        
        try (var writeBuffer = pickler.allocateForWriting(64)) {
            var record = new SimpleRecord(42, "test");
            int bytesWritten = pickler.serialize(writeBuffer, record);
            assertThat(bytesWritten).isEqualTo(9); // ordinal(1) + INT marker(1) + value(1) + STRING marker(1) + length(1) + "test"(4) = 9 bytes
        }
    }
    
    // TODO: Remove premature public API tests - focus on hardening Machinery2 foundation first
    
    @Test
    void testMachinery2BuiltInTypesDiscovery() throws Exception {
        // Test that Machinery2 properly identifies built-in vs user types
        var analysis = Machinery2.analyzeAllReachableTypes(SimpleRecord.class);
        
        // Should discover exactly 1 user type: SimpleRecord
        assertThat(analysis.discoveredClasses()).hasSize(1);
        assertThat(analysis.discoveredClasses()[0]).isEqualTo(SimpleRecord.class);
        
        // Should be lexicographically sorted (trivial with 1 class)
        assertThat(analysis.discoveredClasses()[0].getSimpleName()).isEqualTo("SimpleRecord");
        
        // Should have RECORD tag
        assertThat(analysis.tags()[0]).isEqualTo(Tag.RECORD);
        
        // Should have constructor and component accessors
        assertThat(analysis.constructors()[0]).isNotNull();
        assertThat(analysis.componentAccessors()[0]).hasSize(2); // int value, String name
        
        // Built-in types (int, String) should NOT be in discovered user types
        for (Class<?> clazz : analysis.discoveredClasses()) {
            assertThat(clazz).isNotEqualTo(int.class);
            assertThat(clazz).isNotEqualTo(Integer.class);
            assertThat(clazz).isNotEqualTo(String.class);
        }
    }
    
    @Test
    void testMachinery2ComplexTypesDiscovery() throws Exception {
        // Test complex nested types with arrays, lists, optionals
        var analysis = Machinery2.analyzeAllReachableTypes(ComplexRecord.class);
        
        // Should discover exactly 1 user type: ComplexRecord  
        assertThat(analysis.discoveredClasses()).hasSize(1);
        assertThat(analysis.discoveredClasses()[0]).isEqualTo(ComplexRecord.class);
        
        // Should have 4 component accessors for the 4 fields
        assertThat(analysis.componentAccessors()[0]).hasSize(4);
        
        // Built-in collections should NOT be in discovered types
        for (Class<?> clazz : analysis.discoveredClasses()) {
            assertThat(clazz).isNotEqualTo(List.class);
            assertThat(clazz).isNotEqualTo(Map.class);
            assertThat(clazz).isNotEqualTo(Optional.class);
            assertThat(clazz).isNotEqualTo(int[].class);
        }
    }
    
    @Test
    void testMachinery2NestedRecordsDiscovery() throws Exception {
        // Test nested record discovery
        var analysis = Machinery2.analyzeAllReachableTypes(OuterRecord.class);
        
        // Should discover both records, lexicographically sorted
        assertThat(analysis.discoveredClasses()).hasSize(2);
        assertThat(analysis.discoveredClasses()[0]).isEqualTo(InnerRecord.class); // "InnerRecord" < "OuterRecord"
        assertThat(analysis.discoveredClasses()[1]).isEqualTo(OuterRecord.class);
        
        // Both should be RECORD tags
        assertThat(analysis.tags()[0]).isEqualTo(Tag.RECORD);
        assertThat(analysis.tags()[1]).isEqualTo(Tag.RECORD);
        
        // Component accessors should be correct
        assertThat(analysis.componentAccessors()[0]).hasSize(1); // InnerRecord: String text
        assertThat(analysis.componentAccessors()[1]).hasSize(2); // OuterRecord: InnerRecord inner, int value
    }
    
    @Test
    void testMachinery2EnumDiscovery() throws Exception {
        // Test enum discovery
        var analysis = Machinery2.analyzeAllReachableTypes(EnumRecord.class);
        
        // Should discover both enum and record, lexicographically sorted
        assertThat(analysis.discoveredClasses()).hasSize(2);
        assertThat(analysis.discoveredClasses()[0]).isEqualTo(EnumRecord.class); // "EnumRecord" < "TestStatus"
        assertThat(analysis.discoveredClasses()[1]).isEqualTo(TestStatus.class);
        
        // Correct tags
        assertThat(analysis.tags()[0]).isEqualTo(Tag.RECORD);
        assertThat(analysis.tags()[1]).isEqualTo(Tag.ENUM);
        
        // Enum should have null constructor and componentAccessors
        assertThat(analysis.constructors()[1]).isNull();
        assertThat(analysis.componentAccessors()[1]).isNull();
        
        // Record should have proper constructor and accessors
        assertThat(analysis.constructors()[0]).isNotNull();
        assertThat(analysis.componentAccessors()[0]).hasSize(1); // TestStatus status
    }
    
    @Test
    void testMachinery2OrdinalMapping() throws Exception {
        // Test ordinal assignment correctness
        var analysis = Machinery2.analyzeAllReachableTypes(NestedRecord.class);
        
        // Should discover all 3 records in lexicographical order
        assertThat(analysis.discoveredClasses()).hasSize(3);
        assertThat(analysis.discoveredClasses()[0]).isEqualTo(BRecord.class);   // ordinal 0
        assertThat(analysis.discoveredClasses()[1]).isEqualTo(NestedRecord.class); // ordinal 1  
        assertThat(analysis.discoveredClasses()[2]).isEqualTo(ZRecord.class);   // ordinal 2
        
        // Test ordinal lookup
        assertThat(analysis.getOrdinal(BRecord.class)).isEqualTo(0);
        assertThat(analysis.getOrdinal(NestedRecord.class)).isEqualTo(1);
        assertThat(analysis.getOrdinal(ZRecord.class)).isEqualTo(2);
    }
    
    @Test
    void testLayer2ComprehensiveBuiltInTypes() throws Exception {
        // Test Layer 2: All major built-in types serialization  
        var pickler = Pickler.of(AllBuiltInTypes.class);
        
        try (var writeBuffer = pickler.allocateForWriting(512)) {
            var record = new AllBuiltInTypes(
                true,                           // boolean
                42,                             // int  
                999L,                           // long
                "hello",                        // String
                Optional.of("world"),           // Optional<String>
                List.of(1, 2, 3),              // List<Integer>
                Map.of("key", 100),            // Map<String, Integer>
                new int[]{10, 20},             // int[]
                new String[]{"a", "b"}         // String[]
            );
            
            int bytesWritten = pickler.serialize(writeBuffer, record);
            LOGGER.info(() -> "AllBuiltInTypes serialized to " + bytesWritten + " bytes");
            
            // Should be substantial with all these components
            assertThat(bytesWritten).isGreaterThan(20);
        }
    }
    
    @Test 
    void testLayer2Arrays() throws Exception {
        // Test Layer 2: Array serialization specifically
        var pickler = Pickler.of(ArrayTest.class);
        
        try (var writeBuffer = pickler.allocateForWriting(128)) {
            var record = new ArrayTest(
                new int[]{1, 2, 3},
                new String[]{"a", "b"},  
                new boolean[]{true, false}
            );
            
            int bytesWritten = pickler.serialize(writeBuffer, record);
            LOGGER.info(() -> "ArrayTest serialized to " + bytesWritten + " bytes");
            assertThat(bytesWritten).isGreaterThan(10);
        }
    }
    
    @Test
    void testLayer2Collections() throws Exception {
        // Test Layer 2: Collection serialization specifically
        var pickler = Pickler.of(CollectionTest.class);
        
        try (var writeBuffer = pickler.allocateForWriting(128)) {
            var record = new CollectionTest(
                List.of("hello", "world"),
                Map.of("one", 1, "two", 2)
            );
            
            int bytesWritten = pickler.serialize(writeBuffer, record);
            LOGGER.info(() -> "CollectionTest serialized to " + bytesWritten + " bytes");
            assertThat(bytesWritten).isGreaterThan(15);
        }
    }
    
    @Test
    void testLayer3NestedUserTypes() throws Exception {
        // Test Layer 3: Nested records with user types (records + enums)
        var pickler = Pickler.of(NestedUserTypes.class);
        
        try (var writeBuffer = pickler.allocateForWriting(128)) {
            var inner = new InnerRecord("inner_value");
            var record = new NestedUserTypes(inner, TestEnum.VALUE_A, "outer_name");
            
            int bytesWritten = pickler.serialize(writeBuffer, record);
            LOGGER.info(() -> "NestedUserTypes serialized to " + bytesWritten + " bytes");
            
            // Should handle: ordinal(NestedUserTypes) + ordinal(InnerRecord) + "inner_value" + ordinal(TestEnum) + "VALUE_A" + "outer_name"
            assertThat(bytesWritten).isGreaterThan(20);
        }
    }
    
    @Test 
    void testLayer3RecordWithEnum() throws Exception {
        // Test Layer 3: Simple record containing enum
        var pickler = Pickler.of(Layer3RecordWithEnum.class);
        
        try (var writeBuffer = pickler.allocateForWriting(64)) {
            var record = new Layer3RecordWithEnum(TestEnum.VALUE_B, 42);
            
            int bytesWritten = pickler.serialize(writeBuffer, record);
            LOGGER.info(() -> "RecordWithEnum serialized to " + bytesWritten + " bytes");
            
            // Should handle: ordinal(RecordWithEnum) + ordinal(TestEnum) + "VALUE_B" + int(42)
            assertThat(bytesWritten).isGreaterThan(8);
        }
    }
    
    @Test
    void testLayer4RoundTripEnum() throws Exception {
        // Test Layer 4: Round-trip enum serialization and deserialization
        var pickler = Pickler.of(TestEnum.class);
        
        try (var writeBuffer = pickler.allocateForWriting(64)) {
            var original = TestEnum.VALUE_C;
            
            int bytesWritten = pickler.serialize(writeBuffer, original);
            var readBuffer = pickler.wrapForReading(writeBuffer.flip());
            var deserialized = pickler.deserialize(readBuffer);
            
            LOGGER.info(() -> "Enum round-trip: " + original + " -> " + bytesWritten + " bytes -> " + deserialized);
            assertThat(deserialized).isEqualTo(original);
            assertThat(bytesWritten).isEqualTo(9); // logicalOrdinal(1) + name length(1) + "VALUE_C"(7) = 9 bytes
        }
    }
    
    @Test
    void testLayer4RoundTripNull() throws Exception {
        // Test Layer 4: Round-trip null handling
        var pickler = Pickler.of(TestEnum.class);
        
        try (var writeBuffer = pickler.allocateForWriting(64)) {
            int bytesWritten = pickler.serialize(writeBuffer, null);
            var readBuffer = pickler.wrapForReading(writeBuffer.flip());
            var deserialized = pickler.deserialize(readBuffer);
            
            LOGGER.info(() -> "Null round-trip: null -> " + bytesWritten + " bytes -> " + deserialized);
            assertThat(deserialized).isNull();
            assertThat(bytesWritten).isEqualTo(1); // NULL marker
        }
    }
    
    @Test
    void testLayer4RoundTripSimpleRecord() throws Exception {
        // Test Layer 4: Round-trip simple record with built-ins
        var pickler = Pickler.of(SimpleRecord.class);
        
        try (var writeBuffer = pickler.allocateForWriting(64)) {
            var original = new SimpleRecord(123, "hello");
            
            int bytesWritten = pickler.serialize(writeBuffer, original);
            var readBuffer = pickler.wrapForReading(writeBuffer.flip());
            var deserialized = pickler.deserialize(readBuffer);
            
            LOGGER.info(() -> "SimpleRecord round-trip: " + original + " -> " + bytesWritten + " bytes -> " + deserialized);
            assertThat(deserialized).isEqualTo(original);
            assertThat(deserialized.value()).isEqualTo(123);
            assertThat(deserialized.name()).isEqualTo("hello");
        }
    }
    
    @Test
    void testLayer4RoundTripNestedUserTypes() throws Exception {
        // Test Layer 4: Round-trip complex nested records and enums
        var pickler = Pickler.of(NestedUserTypes.class);
        
        try (var writeBuffer = pickler.allocateForWriting(128)) {
            var inner = new InnerRecord("nested_text");
            var original = new NestedUserTypes(inner, TestEnum.VALUE_A, "outer_text");
            
            int bytesWritten = pickler.serialize(writeBuffer, original);
            var readBuffer = pickler.wrapForReading(writeBuffer.flip());
            var deserialized = pickler.deserialize(readBuffer);
            
            LOGGER.info(() -> "NestedUserTypes round-trip: " + original + " -> " + bytesWritten + " bytes -> " + deserialized);
            assertThat(deserialized).isEqualTo(original);
            assertThat(deserialized.inner().text()).isEqualTo("nested_text");
            assertThat(deserialized.status()).isEqualTo(TestEnum.VALUE_A);
            assertThat(deserialized.name()).isEqualTo("outer_text");
        }
    }
    
    @Test
    void testLayer4RoundTripBuiltInTypes() throws Exception {
        // Test Layer 4: Round-trip comprehensive built-in types
        var pickler = Pickler.of(AllBuiltInTypes.class);
        
        try (var writeBuffer = pickler.allocateForWriting(512)) {
            var original = new AllBuiltInTypes(
                true,                           // boolean
                42,                             // int  
                999L,                           // long
                "hello",                        // String
                Optional.of("world"),           // Optional<String>
                List.of(1, 2, 3),              // List<Integer>
                Map.of("key", 100),            // Map<String, Integer>
                new int[]{10, 20},             // int[]
                new String[]{"a", "b"}         // String[]
            );
            
            int bytesWritten = pickler.serialize(writeBuffer, original);
            var readBuffer = pickler.wrapForReading(writeBuffer.flip());
            var deserialized = pickler.deserialize(readBuffer);
            
            LOGGER.info(() -> "AllBuiltInTypes round-trip: " + bytesWritten + " bytes");
            assertThat(deserialized.flag()).isEqualTo(true);
            assertThat(deserialized.number()).isEqualTo(42);
            assertThat(deserialized.bigNumber()).isEqualTo(999L);
            assertThat(deserialized.text()).isEqualTo("hello");
            assertThat(deserialized.maybeText()).isEqualTo(Optional.of("world"));
            assertThat(deserialized.numbers()).isEqualTo(List.of(1, 2, 3));
            assertThat(deserialized.mapping()).isEqualTo(Map.of("key", 100));
            assertArrayEquals(deserialized.primitiveArray(), new int[]{10, 20});
            assertArrayEquals(deserialized.objectArray(), new String[]{"a", "b"});
        }
    }
}
