// Learning tests documenting how JDK serialization handles backwards compatibility
// These tests demonstrate the differences between JDK's name-based approach and
// No Framework Pickler's position-based approach
package io.github.simbo1905.no.framework;

import org.junit.jupiter.api.Test;

import java.io.*;
import java.lang.reflect.Array;
import java.lang.reflect.RecordComponent;
import java.util.logging.Logger;

import static io.github.simbo1905.no.framework.BackwardsCompatibilityTest.*;
import static org.junit.jupiter.api.Assertions.*;

/// Learning test to understand JDK's built-in serialization backwards compatibility rules
/// for records. This will help us implement similar behavior in No Framework Pickler.
public class JDKRecordBackwardCompatibilityTests {

    static final Logger LOGGER = Logger.getLogger(JDKRecordBackwardCompatibilityTests.class.getName());


    // Original record with one field
    static final String JDK_V1_RECORD = """
        package io.github.simbo1905.no.framework.jdktest;

        import java.io.Serializable;

        public record Person(String name) implements Serializable {
        }
        """;

    // Evolved record with additional field
    static final String JDK_V2_RECORD = """
        package io.github.simbo1905.no.framework.jdktest;

        import java.io.Serializable;

        public record Person(String name, int age) implements Serializable {
            // Compatibility constructor
            public Person(String name) {
                this(name, 0);
            }
        }
        """;

    // Version 3 with even more fields
    static final String JDK_V3_RECORD = """
        package io.github.simbo1905.no.framework.jdktest;

        import java.io.Serializable;

        public record Person(String name, int age, String email) implements Serializable {
            // Compatibility constructors
            public Person(String name) {
                this(name, 0, "unknown@example.com");
            }

            public Person(String name, int age) {
                this(name, age, "unknown@example.com");
            }
        }
        """;

    final String FULL_CLASS_NAME = "io.github.simbo1905.no.framework.jdktest.Person";

    @Test
    void testJDKSerializationV1ToV2() throws Exception {
        // Compile V1
        Class<?> v1Class = compileAndClassLoad(FULL_CLASS_NAME, JDK_V1_RECORD);

        // Create and serialize V1 instance
        Object v1Instance = createRecordInstance(v1Class, new Object[]{"Alice"});
        byte[] serializedData = serializeWithJDK(v1Instance);

        // Compile V2
        Class<?> v2Class = compileAndClassLoad(FULL_CLASS_NAME, JDK_V2_RECORD);

        // Try to deserialize V1 data with V2 class
        try {
            Object v2Instance = deserializeWithJDK(serializedData, v2Class);
            LOGGER.info("Successfully deserialized V1 data into V2 class: " + v2Instance);

            // Check what values we got
            RecordComponent[] components = v2Class.getRecordComponents();
            for (RecordComponent comp : components) {
                Object value = comp.getAccessor().invoke(v2Instance);
                LOGGER.info("Component " + comp.getName() + " = " + value);
            }
        } catch (Exception e) {
            LOGGER.info("Failed to deserialize: " + e.getClass().getName() + " - " + e.getMessage());
            e.printStackTrace();
        }
    }

    @Test
    void testJDKSerializationV2ToV1() throws Exception {
        // Compile V2
        Class<?> v2Class = compileAndClassLoad(FULL_CLASS_NAME, JDK_V2_RECORD);

        // Create and serialize V2 instance
        Object v2Instance = createRecordInstance(v2Class, new Object[]{"Bob", 30});
        byte[] serializedData = serializeWithJDK(v2Instance);

        // Compile V1
        Class<?> v1Class = compileAndClassLoad(FULL_CLASS_NAME, JDK_V1_RECORD);

        // Try to deserialize V2 data with V1 class
        try {
            Object v1Instance = deserializeWithJDK(serializedData, v1Class);
            LOGGER.info("Successfully deserialized V2 data into V1 class: " + v1Instance);
        } catch (Exception e) {
            LOGGER.info("Failed to deserialize: " + e.getClass().getName() + " - " + e.getMessage());
            e.printStackTrace();
        }
    }

    @Test
    void testJDKSerializationWithoutCompatibilityConstructor() throws Exception {
        // V2 without compatibility constructor
        String v2WithoutCompat = """
            package io.github.simbo1905.no.framework.jdktest;

            import java.io.Serializable;

            public record Person(String name, int age) implements Serializable {
                // No compatibility constructor!
            }
            """;

        // Compile V1
        Class<?> v1Class = compileAndClassLoad(FULL_CLASS_NAME, JDK_V1_RECORD);

        // Create and serialize V1 instance
        Object v1Instance = createRecordInstance(v1Class, new Object[]{"Diana"});
        byte[] serializedData = serializeWithJDK(v1Instance);

        // Compile V2 without compatibility constructor
        Class<?> v2Class = compileAndClassLoad(FULL_CLASS_NAME, v2WithoutCompat);

        // Try to deserialize V1 data with V2 class (no compat constructor)
        try {
            Object v2Instance = deserializeWithJDK(serializedData, v2Class);
            LOGGER.info("Unexpectedly succeeded without compat constructor: " + v2Instance);
        } catch (Exception e) {
            LOGGER.info("Failed without compat constructor: " + e.getClass().getName() + " - " + e.getMessage());
        }
    }

    @Test
    void testJDKSerializationInternals() throws Exception {
        // Let's examine how JDK serializes records
        Class<?> v1Class = compileAndClassLoad(FULL_CLASS_NAME, JDK_V1_RECORD);
        Object v1Instance = createRecordInstance(v1Class, new Object[]{"Eve"});

        // Serialize and examine the stream
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (ObjectOutputStream oos = new ObjectOutputStream(baos)) {
            oos.writeObject(v1Instance);
        }
        byte[] data = baos.toByteArray();

        LOGGER.info("Serialized size for V1 record: " + data.length + " bytes");

        // Now serialize V2
        Class<?> v2Class = compileAndClassLoad(FULL_CLASS_NAME, JDK_V2_RECORD);
        Object v2Instance = createRecordInstance(v2Class, new Object[]{"Frank", 35});

        baos = new ByteArrayOutputStream();
        try (ObjectOutputStream oos = new ObjectOutputStream(baos)) {
            oos.writeObject(v2Instance);
        }
        byte[] v2Data = baos.toByteArray();

        LOGGER.info("Serialized size for V2 record: " + v2Data.length + " bytes");

        // Test that records use their canonical constructor
        LOGGER.info("V1 has " + v1Class.getRecordComponents().length + " components");
        LOGGER.info("V2 has " + v2Class.getRecordComponents().length + " components");
    }

    @Test
    void testDefaultValuesForMissingPrimitives() throws Exception {
        // V1 with just one primitive
        String v1 = """
            package io.github.simbo1905.no.framework.jdktest;
            import java.io.Serializable;
            public record DataRecord(int count) implements Serializable {}
            """;

        // V2 with additional primitives - let's log what JDK sets them to
        String v2 = """
            package io.github.simbo1905.no.framework.jdktest;
            import java.io.Serializable;
            import java.util.logging.Logger;

            public record DataRecord(
                int count,
                boolean flag,
                byte b,
                short s,
                long l,
                float f,
                double d,
                char c
            ) implements Serializable {
                private static final Logger LOGGER = Logger.getLogger(DataRecord.class.getName());

                // Canonical constructor that logs what JDK set before construction
                public DataRecord {
                    LOGGER.info("JDK set values before canonical constructor: " +
                        "count=" + count +
                        ", flag=" + flag +
                        ", b=" + b +
                        ", s=" + s +
                        ", l=" + l +
                        ", f=" + f +
                        ", d=" + d +
                        ", c=" + ((int)c) + " (char code)");
                }
            }
            """;

        Class<?> v1Class = compileAndClassLoad("io.github.simbo1905.no.framework.jdktest.DataRecord", v1);
        Object v1Instance = createRecordInstance(v1Class, new Object[]{42});
        byte[] data = serializeWithJDK(v1Instance);

        Class<?> v2Class = compileAndClassLoad("io.github.simbo1905.no.framework.jdktest.DataRecord", v2);
        Object v2Instance = deserializeWithJDK(data, v2Class);

        LOGGER.info("Deserialized v2 instance: " + v2Instance);
    }

    @Test
    void testDefaultValuesForMissingReferences() throws Exception {
        // V1 with primitive
        String v1 = """
            package io.github.simbo1905.no.framework.jdktest;
            import java.io.Serializable;
            public record RefRecord(String name) implements Serializable {}
            """;

        // V2 with additional reference types
        String v2 = """
            package io.github.simbo1905.no.framework.jdktest;
            import java.io.Serializable;
            import java.util.List;
            import java.util.Map;
            import java.util.logging.Logger;

            public record RefRecord(
                String name,
                String description,
                List<String> tags,
                Map<String, Integer> scores,
                Object data
            ) implements Serializable {
                private static final Logger LOGGER = Logger.getLogger(RefRecord.class.getName());

                public RefRecord {
                    LOGGER.info("JDK set reference values: " +
                        "name=" + name +
                        ", description=" + description +
                        ", tags=" + tags +
                        ", scores=" + scores +
                        ", data=" + data);
                }
            }
            """;

        Class<?> v1Class = compileAndClassLoad("io.github.simbo1905.no.framework.jdktest.RefRecord", v1);
        Object v1Instance = createRecordInstance(v1Class, new Object[]{"test"});
        byte[] data = serializeWithJDK(v1Instance);

        Class<?> v2Class = compileAndClassLoad("io.github.simbo1905.no.framework.jdktest.RefRecord", v2);
        Object v2Instance = deserializeWithJDK(data, v2Class);

        LOGGER.info("Deserialized reference record: " + v2Instance);
    }

    @Test
    void testDefaultValuesForMissingArrays() throws Exception {
        // V1 with one field
        String v1 = """
            package io.github.simbo1905.no.framework.jdktest;
            import java.io.Serializable;
            public record ArrayRecord(String id) implements Serializable {}
            """;

        // V2 with arrays
        String v2 = """
            package io.github.simbo1905.no.framework.jdktest;
            import java.io.Serializable;
            import java.util.Arrays;
            import java.util.logging.Logger;

            public record ArrayRecord(
                String id,
                int[] numbers,
                String[] names,
                boolean[] flags,
                Object[] objects
            ) implements Serializable {
                private static final Logger LOGGER = Logger.getLogger(ArrayRecord.class.getName());

                public ArrayRecord {
                    LOGGER.info("JDK set array values: " +
                        "id=" + id +
                        ", numbers=" + numbers +
                        ", names=" + names +
                        ", flags=" + flags +
                        ", objects=" + objects);
                }
            }
            """;

        Class<?> v1Class = compileAndClassLoad("io.github.simbo1905.no.framework.jdktest.ArrayRecord", v1);
        Object v1Instance = createRecordInstance(v1Class, new Object[]{"test-id"});
        byte[] data = serializeWithJDK(v1Instance);

        Class<?> v2Class = compileAndClassLoad("io.github.simbo1905.no.framework.jdktest.ArrayRecord", v2);
        Object v2Instance = deserializeWithJDK(data, v2Class);

        LOGGER.info("Deserialized array record: " + v2Instance);
    }

    @Test
    void testDangerousFieldInsertion() throws Exception {
        // Original: int a, int b
        String v1 = """
            package io.github.simbo1905.no.framework.jdktest;
            import java.io.Serializable;
            public record Dangerous(int a, int b) implements Serializable {}
            """;

        // Insert double in middle: int a, double c, int b
        String v2 = """
            package io.github.simbo1905.no.framework.jdktest;
            import java.io.Serializable;
            import java.util.logging.Logger;

            public record Dangerous(int a, double c, int b) implements Serializable {
                private static final Logger LOGGER = Logger.getLogger(Dangerous.class.getName());

                public Dangerous {
                    LOGGER.info("DANGEROUS insertion test - JDK set: a=" + a + ", c=" + c + ", b=" + b);
                }
            }
            """;

        Class<?> v1Class = compileAndClassLoad("io.github.simbo1905.no.framework.jdktest.Dangerous", v1);
        Object v1Instance = createRecordInstance(v1Class, new Object[]{100, 200});
        byte[] data = serializeWithJDK(v1Instance);

        LOGGER.info("V1 instance before serialization: " + v1Instance);

        Class<?> v2Class = compileAndClassLoad("io.github.simbo1905.no.framework.jdktest.Dangerous", v2);
        try {
            Object v2Instance = deserializeWithJDK(data, v2Class);
            LOGGER.info("DANGEROUS: Succeeded! Result: " + v2Instance);
        } catch (Exception e) {
            LOGGER.info("DANGEROUS: Failed as expected: " + e.getClass().getName() + " - " + e.getMessage());
        }
    }

    @Test
    void testCanonicalConstructorDefaultHandling() throws Exception {
        // V1 without department
        String v1 = """
            package com.example.protocol;
            import java.io.Serializable;

            public record UserInfo(String name, int accessLevel) implements Serializable {}
            """;

        // V2 with canonical constructor that converts null to logical default
        String v2 = """
            package com.example.protocol;
            import java.io.Serializable;
            import java.util.logging.Logger;

            public record UserInfo(String name, int accessLevel, String department) implements Serializable {
                private static final Logger LOGGER = Logger.getLogger(UserInfo.class.getName());

                // Canonical constructor that converts JDK's null default to logical default
                public UserInfo(String name, int accessLevel, String department) {
                    LOGGER.info("Canonical constructor called with: name=" + name +
                               ", accessLevel=" + accessLevel + ", department=" + department);
                    this.name = name;
                    this.accessLevel = accessLevel;
                    this.department = department == null ? "Unknown" : department;
                    LOGGER.info("After constructor: department=" + this.department);
                }
            }
            """;

        Class<?> v1Class = compileAndClassLoad("com.example.protocol.UserInfo", v1);
        Object v1Instance = createRecordInstance(v1Class, new Object[]{"Alice", 5});
        byte[] data = serializeWithJDK(v1Instance);

        Class<?> v2Class = compileAndClassLoad("com.example.protocol.UserInfo", v2);
        Object v2Instance = deserializeWithJDK(data, v2Class);

        // Verify the department field was set to "Unknown" not null
        RecordComponent[] components = v2Class.getRecordComponents();
        for (RecordComponent comp : components) {
            if (comp.getName().equals("department")) {
                Object value = comp.getAccessor().invoke(v2Instance);
                assertEquals("Unknown", value, "Department should be 'Unknown', not null");
                LOGGER.info("Verified: department field is '" + value + "'");
            }
        }

        LOGGER.info("Final deserialized record: " + v2Instance);
    }

    @Test
    void testJDKSerializationFieldOrder() throws Exception {
        // Test with reordered fields
        String reorderedRecord = """
            package io.github.simbo1905.no.framework.jdktest;

            import java.io.Serializable;

            public record Person(int age, String name) implements Serializable {
            }
            """;

        // Compile original V2
        Class<?> v2Class = compileAndClassLoad(FULL_CLASS_NAME, JDK_V2_RECORD);

        // Create and serialize V2 instance
        Object v2Instance = createRecordInstance(v2Class, new Object[]{"Charlie", 25});
        byte[] serializedData = serializeWithJDK(v2Instance);

        // Compile reordered version
        Class<?> reorderedClass = compileAndClassLoad(FULL_CLASS_NAME, reorderedRecord);

        // Try to deserialize
        try {
            Object reorderedInstance = deserializeWithJDK(serializedData, reorderedClass);
            LOGGER.info("Successfully deserialized into reordered class: " + reorderedInstance);
        } catch (Exception e) {
            LOGGER.info("Failed with reordered fields: " + e.getClass().getName() + " - " + e.getMessage());
        }
    }

    /// Serialize using standard JDK serialization
    private byte[] serializeWithJDK(Object obj) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (ObjectOutputStream oos = new ObjectOutputStream(baos)) {
            oos.writeObject(obj);
        }
        return baos.toByteArray();
    }

    /// Deserialize using standard JDK serialization with custom class resolution
    private Object deserializeWithJDK(byte[] data, Class<?> expectedClass) throws IOException, ClassNotFoundException {
        ByteArrayInputStream bais = new ByteArrayInputStream(data);
        try (ObjectInputStream ois = new ObjectInputStream(bais) {
            @Override
            protected Class<?> resolveClass(ObjectStreamClass desc) throws IOException, ClassNotFoundException {
                // Try to use the expected class if the name matches
                if (desc.getName().equals(expectedClass.getName())) {
                    return expectedClass;
                }
                return super.resolveClass(desc);
            }
        }) {
            return ois.readObject();
        }
    }

    // ========== ENUM LEARNING TESTS ==========

    @Test
    void testJDKEnumSerializationBasics() throws Exception {
        // V1: Basic enum
        String v1Enum = """
            package io.github.simbo1905.no.framework.jdktest;
            import java.io.Serializable;
            public enum Status implements Serializable {
                ACTIVE, INACTIVE, PENDING
            }
            """;

        Class<?> v1Class = compileAndClassLoad("io.github.simbo1905.no.framework.jdktest.Status", v1Enum);
        Object activeStatus = v1Class.getEnumConstants()[0]; // ACTIVE

        byte[] data = serializeWithJDK(activeStatus);
        LOGGER.info("Enum ACTIVE serialized to " + data.length + " bytes");

        Object deserialized = deserializeWithJDK(data, v1Class);
        LOGGER.info("Deserialized enum: " + deserialized);
        assertEquals("ACTIVE", deserialized.toString());
    }

    @Test
    void testJDKEnumReordering() throws Exception {
        // V1: Original order
        String v1Enum = """
            package io.github.simbo1905.no.framework.jdktest;
            import java.io.Serializable;
            public enum Priority implements Serializable {
                LOW, MEDIUM, HIGH
            }
            """;

        // V2: Reordered
        String v2Enum = """
            package io.github.simbo1905.no.framework.jdktest;
            import java.io.Serializable;
            public enum Priority implements Serializable {
                HIGH, MEDIUM, LOW  // Reordered!
            }
            """;

        Class<?> v1Class = compileAndClassLoad("io.github.simbo1905.no.framework.jdktest.Priority", v1Enum);
        Object mediumV1 = v1Class.getEnumConstants()[1]; // MEDIUM (ordinal 1 in V1)

        byte[] data = serializeWithJDK(mediumV1);

        Class<?> v2Class = compileAndClassLoad("io.github.simbo1905.no.framework.jdktest.Priority", v2Enum);
        Object deserializedV2 = deserializeWithJDK(data, v2Class);

        LOGGER.info("V1 MEDIUM (ordinal 1) deserialized to V2: " + deserializedV2 +
                   " with ordinal: " + ((Enum<?>)deserializedV2).ordinal());

        // JDK serialization uses names, so MEDIUM remains MEDIUM even though ordinal changed
        assertEquals("MEDIUM", deserializedV2.toString());
        assertEquals(1, ((Enum<?>)mediumV1).ordinal()); // V1 MEDIUM ordinal
        assertEquals(1, ((Enum<?>)deserializedV2).ordinal()); // V2 MEDIUM ordinal (still 1)
    }

    @Test
    void testJDKEnumRenaming() throws Exception {
        // V1: Original names
        String v1Enum = """
            package io.github.simbo1905.no.framework.jdktest;
            import java.io.Serializable;
            public enum State implements Serializable {
                STARTED, STOPPED, PAUSED
            }
            """;

        // V2: Renamed constant
        String v2Enum = """
            package io.github.simbo1905.no.framework.jdktest;
            import java.io.Serializable;
            public enum State implements Serializable {
                RUNNING, STOPPED, PAUSED  // STARTED renamed to RUNNING
            }
            """;

        Class<?> v1Class = compileAndClassLoad("io.github.simbo1905.no.framework.jdktest.State", v1Enum);
        Object startedV1 = v1Class.getEnumConstants()[0]; // STARTED

        byte[] data = serializeWithJDK(startedV1);

        Class<?> v2Class = compileAndClassLoad("io.github.simbo1905.no.framework.jdktest.State", v2Enum);

        try {
            Object deserializedV2 = deserializeWithJDK(data, v2Class);
            LOGGER.info("Unexpectedly succeeded deserializing renamed enum: " + deserializedV2);
            fail("Should have failed with renamed enum constant");
        } catch (Exception e) {
            LOGGER.info("Failed as expected with renamed enum: " + e.getClass().getName() + " - " + e.getMessage());
            // Expected: java.lang.IllegalArgumentException: No enum constant State.STARTED
        }
    }

    @Test
    void testJDKEnumAddingConstants() throws Exception {
        // V1: Three constants
        String v1Enum = """
            package io.github.simbo1905.no.framework.jdktest;
            import java.io.Serializable;
            public enum Level implements Serializable {
                INFO, WARN, ERROR
            }
            """;

        // V2: Added DEBUG at beginning
        String v2Enum = """
            package io.github.simbo1905.no.framework.jdktest;
            import java.io.Serializable;
            public enum Level implements Serializable {
                DEBUG, INFO, WARN, ERROR  // Added DEBUG
            }
            """;

        Class<?> v1Class = compileAndClassLoad("io.github.simbo1905.no.framework.jdktest.Level", v1Enum);
        Object warnV1 = v1Class.getEnumConstants()[1]; // WARN (ordinal 1 in V1)

        byte[] data = serializeWithJDK(warnV1);

        Class<?> v2Class = compileAndClassLoad("io.github.simbo1905.no.framework.jdktest.Level", v2Enum);
        Object deserializedV2 = deserializeWithJDK(data, v2Class);

        LOGGER.info("V1 WARN deserialized to V2: " + deserializedV2 +
                   " with ordinal: " + ((Enum<?>)deserializedV2).ordinal());

        // JDK uses names, so WARN is still WARN but ordinal changed from 1 to 2
        assertEquals("WARN", deserializedV2.toString());
        assertEquals(1, ((Enum<?>)warnV1).ordinal()); // V1 WARN ordinal
        assertEquals(2, ((Enum<?>)deserializedV2).ordinal()); // V2 WARN ordinal (shifted by DEBUG)
    }

    @Test
    void testJDKEnumRemovingConstants() throws Exception {
        // V1: Four constants
        String v1Enum = """
            package io.github.simbo1905.no.framework.jdktest;
            import java.io.Serializable;
            public enum Mode implements Serializable {
                READ, WRITE, APPEND, DELETE
            }
            """;

        // V2: Removed APPEND
        String v2Enum = """
            package io.github.simbo1905.no.framework.jdktest;
            import java.io.Serializable;
            public enum Mode implements Serializable {
                READ, WRITE, DELETE  // Removed APPEND
            }
            """;

        Class<?> v1Class = compileAndClassLoad("io.github.simbo1905.no.framework.jdktest.Mode", v1Enum);
        Object appendV1 = v1Class.getEnumConstants()[2]; // APPEND

        byte[] data = serializeWithJDK(appendV1);

        Class<?> v2Class = compileAndClassLoad("io.github.simbo1905.no.framework.jdktest.Mode", v2Enum);

        try {
            Object deserializedV2 = deserializeWithJDK(data, v2Class);
            LOGGER.info("Unexpectedly succeeded deserializing removed enum: " + deserializedV2);
            fail("Should have failed with removed enum constant");
        } catch (Exception e) {
            LOGGER.info("Failed as expected with removed enum: " + e.getClass().getName() + " - " + e.getMessage());
            // Expected: java.lang.IllegalArgumentException: No enum constant Mode.APPEND
        }
    }

    @Test
    void testJDKEnumInRecord() throws Exception {
        // Test enum reordering with records - simpler test case
        // V1: enum with original order
        String v1Enum = """
            package io.github.simbo1905.no.framework.jdktest;
            import java.io.Serializable;
            public enum Color implements Serializable {
                RED, GREEN, BLUE
            }
            """;

        // V2: enum with reordered constants
        String v2Enum = """
            package io.github.simbo1905.no.framework.jdktest;
            import java.io.Serializable;
            public enum Color implements Serializable {
                BLUE, RED, GREEN  // Reordered
            }
            """;

        Class<?> v1Class = compileAndClassLoad("io.github.simbo1905.no.framework.jdktest.Color", v1Enum);
        Object greenV1 = v1Class.getEnumConstants()[1]; // GREEN (ordinal 1 in V1)

        byte[] data = serializeWithJDK(greenV1);

        Class<?> v2Class = compileAndClassLoad("io.github.simbo1905.no.framework.jdktest.Color", v2Enum);
        Object greenV2 = deserializeWithJDK(data, v2Class);

        LOGGER.info("V1 GREEN (ordinal 1) deserialized to V2: " + greenV2 +
                   " with ordinal: " + ((Enum<?>)greenV2).ordinal());

        // JDK preserves enum by name, so GREEN is still GREEN but ordinal changed
        assertEquals("GREEN", greenV2.toString());
        assertEquals(1, ((Enum<?>)greenV1).ordinal()); // V1 GREEN ordinal
        assertEquals(2, ((Enum<?>)greenV2).ordinal()); // V2 GREEN ordinal (now at position 2)
    }

    @Test
    void testJDKEnumArraySerialization() throws Exception {
        // Test how JDK serializes enum arrays
        String enumDef = """
            package io.github.simbo1905.no.framework.jdktest;
            import java.io.Serializable;
            public enum Color implements Serializable {
                RED, GREEN, BLUE
            }
            """;

        Class<?> colorClass = compileAndClassLoad("io.github.simbo1905.no.framework.jdktest.Color", enumDef);
        Object[] colors = colorClass.getEnumConstants();

        // Single enum
        byte[] singleData = serializeWithJDK(colors[1]); // GREEN
        LOGGER.info("Single enum GREEN serialized to " + singleData.length + " bytes");

        // Array of enums
        Object colorArray = Array.newInstance(colorClass, 3);
        Array.set(colorArray, 0, colors[0]); // RED
        Array.set(colorArray, 1, colors[1]); // GREEN
        Array.set(colorArray, 2, colors[2]); // BLUE

        byte[] arrayData = serializeWithJDK(colorArray);
        LOGGER.info("Enum array [RED, GREEN, BLUE] serialized to " + arrayData.length + " bytes");

        // JDK writes enum names for both single and array cases
        LOGGER.info("JDK enum serialization uses names, not ordinals");
    }

}
