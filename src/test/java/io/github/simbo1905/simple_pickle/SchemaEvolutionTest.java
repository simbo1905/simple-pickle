// SPDX-FileCopyrightText: 2025 Simon Massey
// SPDX-License-Identifier: Apache-2.0
package io.github.simbo1905.simple_pickle;

import org.junit.jupiter.api.Test;

import javax.tools.*;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.lang.reflect.RecordComponent;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.*;

/// Tests for schema evolution in records.
/// This class demonstrates how records can evolve over time while maintaining
/// compatibility with previously serialized data.
public class SchemaEvolutionTest {

    static final Logger LOGGER = Logger.getLogger(SchemaEvolutionTest.class.getName());

    /// Original schema with just one field
    static final String ORIGINAL_SCHEMA = """
            package io.github.simbo1905.simple_pickle.evolution;
            
            /// A simple record with a single field.
            public record TestRecord(int myInt) {
            }
            """;

    /// Evolved schema with an additional field and backward compatibility constructor
    /// IMPORTANT note that it has renamed the int field to `blah` it is only changing the type or reordering that is a problem.
    static final String EVOLVED_SCHEMA = """
        package io.github.simbo1905.simple_pickle.evolution;
        
        /// An evolved record with an additional field and backward compatibility.
        public record TestRecord(int blah, int myNewInt) {
                /// Backward compatibility constructor that sets default value for new field.
                public TestRecord(int xxx) {
                    this(xxx, 42); // Default value for the new field
                }
            }
        """;

    /// Tests schema evolution by:
    /// 1. Compiling and loading the original schema
    /// 2. Creating and serializing an instance
    /// 3. Compiling and loading the evolved schema
    /// 4. Deserializing the original data with the evolved schema
    /// 5. Verifying all fields are correctly populated
    @Test
    void testBasicSchemaEvolution() throws Exception {
        // Step 1: Compile and load the original schema
        Class<?> originalClass = compileAndLoadClass(ORIGINAL_SCHEMA);
        assertTrue(originalClass.isRecord(), "Compiled class should be a record");
        
        // Step 2: Create an instance of the original record
        Object originalInstance = createRecordInstance(originalClass, new Object[]{123});
        LOGGER.fine("Original instance: " + originalInstance);
        
        // Step 3: Serialize the original instance
        byte[] serializedData = serializeRecord(originalInstance);
        
        // Step 4: Compile and load the evolved schema
        Class<?> evolvedClass = compileAndLoadClass(EVOLVED_SCHEMA);
        assertTrue(evolvedClass.isRecord(), "Evolved class should be a record");
        
        // Step 5: Deserialize using the evolved schema
        Object evolvedInstance = deserializeRecord(evolvedClass, serializedData);
        LOGGER.fine("Evolved instance: " + evolvedInstance);
        
        // Step 6: Verify the fields
        verifyRecordComponents(evolvedInstance, Map.of(
            "myInt", 123,
            "myNewInt", 42  // Should be the default value from the compatibility constructor
        ));
    }

    /// Tests round-trip serialization with the evolved schema.
    @Test
    void testRoundTripWithEvolvedSchema() throws Exception {
        // Compile and load the evolved schema
        Class<?> evolvedClass = compileAndLoadClass(EVOLVED_SCHEMA);
        
        // Create an instance with both fields specified
        Object originalInstance = createRecordInstance(evolvedClass, new Object[]{123, 456});
        LOGGER.fine("Original evolved instance: " + originalInstance);
        
        // Serialize and then deserialize
        byte[] serializedData = serializeRecord(originalInstance);
        Object deserializedInstance = deserializeRecord(evolvedClass, serializedData);
        LOGGER.fine("Deserialized evolved instance: " + deserializedInstance);
        
        // Verify both fields are preserved
        verifyRecordComponents(deserializedInstance, Map.of(
            "myInt", 123,
            "myNewInt", 456
        ));
    }

    /// Tests serializing with evolved schema and deserializing with original schema.
    /// This should fail as the original schema doesn't know about the new field.
    /// 
    /// Note: With the updated Pickler implementation, the exception is now a RuntimeException
    /// with a specific message about schema evolution.
    @Test
    void testBackwardIncompatibility() throws Exception {
        // Compile and load both schemas
        Class<?> originalClass = compileAndLoadClass(ORIGINAL_SCHEMA);
        Class<?> evolvedClass = compileAndLoadClass(EVOLVED_SCHEMA);
        
        // Create an instance of the evolved record
        Object evolvedInstance = createRecordInstance(evolvedClass, new Object[]{123, 456});
        
        // Serialize the evolved instance
        byte[] serializedData = serializeRecord(evolvedInstance);
        
        // Attempting to deserialize with the original schema should fail
        Exception exception = assertThrows(RuntimeException.class, 
                () -> deserializeRecord(originalClass, serializedData));
        
        LOGGER.fine("Expected exception: " + exception.getMessage());
        // The exception should contain a message about schema evolution
        assertTrue(exception.getMessage().contains("Schema evolution error") || 
                   exception.getMessage().contains("Failed to create instance"),
                "Exception should be related to schema evolution or constructor mismatch");
    }

    /// Compiles and loads a class from source code.
    ///
    /// @param sourceCode The source code
    /// @return The loaded class
    public static Class<?> compileAndLoadClass(String sourceCode) throws Exception {
        final String fullClassName = "io.github.simbo1905.simple_pickle.evolution.TestRecord";
        
        // Compile the source code
        byte[] classBytes = compileSource(sourceCode, fullClassName);
        
        // Load the compiled class
        return loadCompiledClass(fullClassName, classBytes);
    }
    
    /// Compiles source code into bytecode.
    ///
    /// @param sourceCode The source code to compile
    /// @param fullClassName The fully qualified class name
    /// @return The compiled bytecode
    public static byte[] compileSource(String sourceCode, String fullClassName) {
        // Get the Java compiler
        JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
        if (compiler == null) {
            throw new IllegalStateException("Java compiler not available. Make sure you're running with JDK.");
        }

        // Set up the compilation
        DiagnosticCollector<JavaFileObject> diagnostics = new DiagnosticCollector<>();
        InMemoryFileManager fileManager = new InMemoryFileManager(
            compiler.getStandardFileManager(diagnostics, null, null));

        // Create the source file object
        JavaFileObject sourceFile = new InMemorySourceFile(fullClassName, sourceCode);

        // Compile the source
        JavaCompiler.CompilationTask task = compiler.getTask(
            null, fileManager, diagnostics, null, null, List.of(sourceFile));

        boolean success = task.call();
        if (!success) {
            throwCompilationError(diagnostics);
        }

        // Return the compiled bytecode
        return fileManager.getClassBytes(fullClassName);
    }
    
    /// Throws a runtime exception with compilation error details.
    ///
    /// @param diagnostics The compilation diagnostics
    public static void throwCompilationError(DiagnosticCollector<JavaFileObject> diagnostics) {
        StringBuilder errorMsg = new StringBuilder("Compilation failed:\n");
        for (Diagnostic<? extends JavaFileObject> diagnostic : diagnostics.getDiagnostics()) {
            errorMsg.append(diagnostic).append("\n");
        }
        throw new RuntimeException(errorMsg.toString());
    }
    
    /// Loads a class from its bytecode.
    ///
    /// @param fullClassName The fully qualified class name
    /// @param classBytes The class bytecode
    /// @return The loaded class
    public static Class<?> loadCompiledClass(String fullClassName, byte[] classBytes) throws ClassNotFoundException {
        InMemoryClassLoader classLoader = new InMemoryClassLoader(
            SchemaEvolutionTest.class.getClassLoader(), 
            Map.of(fullClassName, classBytes));
        
        return classLoader.loadClass(fullClassName);
    }

    /// Creates an instance of a record using reflection.
    /// 
    /// @param recordClass The record class
    /// @param args The constructor arguments
    /// @return A new instance of the record
    public static Object createRecordInstance(Class<?> recordClass, Object[] args) throws Exception {
        // Get constructor matching the record components
        Constructor<?> constructor = getRecordConstructor(recordClass);
        
        // Create and return the instance
        return constructor.newInstance(args);
    }
    
    /// Gets the canonical constructor for a record class.
    ///
    /// @param recordClass The record class
    /// @return The canonical constructor
    public static Constructor<?> getRecordConstructor(Class<?> recordClass) throws NoSuchMethodException {
        // Get the record components to determine constructor parameter types
        RecordComponent[] components = recordClass.getRecordComponents();
        Class<?>[] paramTypes = Arrays.stream(components)
            .map(RecordComponent::getType)
            .toArray(Class<?>[]::new);

        // Get the constructor matching the component types
        return recordClass.getDeclaredConstructor(paramTypes);
    }

    /// Serializes a record instance using the Pickler.
    /// 
    /// @param record The record instance
    /// @return The serialized bytes
    @SuppressWarnings({"unchecked", "rawtypes"})
    public static byte[] serializeRecord(Object record) {
        // Get the pickler for the record class
        Class<? extends Record> recordClass = (Class<? extends Record>) record.getClass();
      Pickler pickler = Pickler.forRecord(recordClass);
        
        // Calculate buffer size and allocate buffer
        int size = pickler.sizeOf(record);
        ByteBuffer buffer = ByteBuffer.allocate(size);
        
        // Serialize the record
        pickler.serialize(record, buffer);
        buffer.flip();
        
        // Return the serialized bytes
        byte[] result = new byte[buffer.remaining()];
        buffer.get(result);
        return result;
    }

    /// Deserializes a record from bytes using the Pickler.
    /// 
    /// @param recordClass The record class
    /// @param bytes The serialized bytes
    /// @return The deserialized record instance
    @SuppressWarnings({"unchecked", "rawtypes"})
    public static Object deserializeRecord(Class<?> recordClass, byte[] bytes) {
        // Get the pickler for the record class
      Pickler pickler = Pickler.forRecord((Class<? extends Record>) recordClass);
        
        // Wrap the bytes in a buffer
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        
        // Deserialize the record
        return pickler.deserialize(buffer);
    }

    /// Verifies that a record instance has the expected component values.
    /// 
    /// @param record The record instance
    /// @param expectedValues Map of component names to expected values
    public static void verifyRecordComponents(Object record, Map<String, Object> expectedValues) {
        Class<?> recordClass = record.getClass();
        RecordComponent[] components = recordClass.getRecordComponents();
        
        Arrays.stream(components)
            .filter(component -> expectedValues.containsKey(component.getName()))
            .forEach(component -> verifyComponent(record, component, expectedValues));
    }
    
    /// Verifies a single record component value.
    ///
    /// @param record The record instance
    /// @param component The record component to verify
    /// @param expectedValues Map of component names to expected values
    public static void verifyComponent(Object record, RecordComponent component, Map<String, Object> expectedValues) {
        String name = component.getName();
        try {
            Method accessor = component.getAccessor();
            Object actualValue = accessor.invoke(record);
            Object expectedValue = expectedValues.get(name);
            
            assertEquals(expectedValue, actualValue, 
                "Component '" + name + "' has value " + actualValue + 
                " but expected " + expectedValue);
            
            LOGGER.fine("Verified component '" + name + "' = " + actualValue);
        } catch (Exception e) {
            fail("Failed to access component '" + name + "': " + e.getMessage());
        }
    }

    // Inner classes for in-memory compilation

    /// A JavaFileObject implementation that holds source code in memory.
    public static class InMemorySourceFile extends SimpleJavaFileObject {
        private final String code;

        InMemorySourceFile(String className, String code) {
            super(URI.create("string:///" + className.replace('.', '/') + Kind.SOURCE.extension), 
                  Kind.SOURCE);
            this.code = code;
        }

        @Override
        public CharSequence getCharContent(boolean ignoreEncodingErrors) {
            return code;
        }
    }

    /// A JavaFileObject implementation that collects compiled bytecode in memory.
    public static class InMemoryClassFile extends SimpleJavaFileObject {
        private final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

        InMemoryClassFile(String className) {
            super(URI.create("bytes:///" + className.replace('.', '/') + Kind.CLASS.extension), 
                  Kind.CLASS);
        }

        byte[] getBytes() {
            return outputStream.toByteArray();
        }

        @Override
        public OutputStream openOutputStream() {
            return outputStream;
        }
    }

    /// A JavaFileManager that keeps compiled classes in memory.
    public static class InMemoryFileManager extends ForwardingJavaFileManager<StandardJavaFileManager> {
        private final Map<String, InMemoryClassFile> classFiles = new HashMap<>();

        InMemoryFileManager(StandardJavaFileManager fileManager) {
            super(fileManager);
        }

        @Override
        public JavaFileObject getJavaFileForOutput(Location location, String className, 
                                                  JavaFileObject.Kind kind, FileObject sibling) {
            if (kind == JavaFileObject.Kind.CLASS) {
                InMemoryClassFile classFile = new InMemoryClassFile(className);
                classFiles.put(className, classFile);
                return classFile;
            }
            try {
                return super.getJavaFileForOutput(location, className, kind, sibling);
            } catch (IOException e) {
                throw new RuntimeException("Failed to get file for output", e);
            }
        }

        byte[] getClassBytes(String className) {
            InMemoryClassFile file = classFiles.get(className);
            if (file == null) {
                throw new IllegalArgumentException("No class file for: " + className);
            }
            return file.getBytes();
        }
    }

    /// A ClassLoader that loads classes from in-memory bytecode.
    public static class InMemoryClassLoader extends ClassLoader {
        private final Map<String, byte[]> classBytes;

        InMemoryClassLoader(ClassLoader parent, Map<String, byte[]> classBytes) {
            super(parent);
            this.classBytes = new HashMap<>(classBytes);
        }

        @Override
        protected Class<?> findClass(String name) throws ClassNotFoundException {
            byte[] bytes = classBytes.get(name);
            if (bytes != null) {
                return defineClass(name, bytes, 0, bytes.length);
            }
            return super.findClass(name);
        }
    }
}
