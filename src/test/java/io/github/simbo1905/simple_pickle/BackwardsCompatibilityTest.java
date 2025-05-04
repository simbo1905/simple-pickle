// SPDX-FileCopyrightText: 2025 Simon Massey
// SPDX-License-Identifier: Apache-2.0

package io.github.simbo1905.simple_pickle;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.tools.DiagnosticCollector;
import javax.tools.JavaCompiler;
import javax.tools.JavaFileObject;
import javax.tools.ToolProvider;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import static io.github.simbo1905.simple_pickle.SchemaEvolutionTest.*;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests specifically focused on the BACKWARDS compatibility mode.
 * This test suite verifies that records can evolve by adding new fields
 * while maintaining the ability to deserialize data serialized with older schemas.
 */
public class BackwardsCompatibilityTest {

  static final Logger LOGGER = Logger.getLogger(BackwardsCompatibilityTest.class.getName());

  // Original schema with just one field
  static final String ORIGINAL_SCHEMA = """
      package io.github.simbo1905.simple_pickle.evolution;
      
      /**
       * A simple record with a single field.
       */
      public record SimpleRecord(int value) {
      }
      """;

  // Evolved schema with two additional fields and backward compatibility constructor
  static final String EVOLVED_SCHEMA = """
      package io.github.simbo1905.simple_pickle.evolution;
      
      /**
       * An evolved record with additional fields and backward compatibility.
       */
      public record SimpleRecord(int value, String name, double score) {
          /**
           * Backward compatibility constructor for original schema.
           */
          public SimpleRecord(int value) {
              this(value, "default", 0.0);
          }
      }
      """;

  // Further evolved schema with even more fields
  static final String FURTHER_EVOLVED_SCHEMA = """
      package io.github.simbo1905.simple_pickle.evolution;
      
      import java.time.Instant;
      
      /**
       * A further evolved record with more fields and backward compatibility.
       */
      public record SimpleRecord(int value, String name, double score, boolean active, long timestamp) {
          /**
           * Backward compatibility constructor for original schema.
           */
          public SimpleRecord(int value) {
              this(value, "default", 0.0, true, System.currentTimeMillis());
          }
      
          /**
           * Backward compatibility constructor for first evolution.
           */
          public SimpleRecord(int value, String name, double score) {
              this(value, name, score, true, System.currentTimeMillis());
          }
      }
      """;

  final String FULL_CLASS_NAME = "io.github.simbo1905.simple_pickle.evolution.SimpleRecord";

  @BeforeEach
  void setUp() {
    System.setProperty(Pickler.COMPATIBILITY_SYSTEM_PROPERTY, Pickler.Compatibility.BACKWARDS.name());
  }

  @AfterEach
  void tearDown() {
    System.clearProperty(Pickler.COMPATIBILITY_SYSTEM_PROPERTY);
  }

  @Test
  void testOriginalToEvolvedDeserializationStrict() throws Exception {
    // clear should force strict mode
    System.clearProperty(Pickler.COMPATIBILITY_SYSTEM_PROPERTY);
    // Compile and class load
    Class<?> originalClass = compileAndClassLoad(compiler, FULL_CLASS_NAME, ORIGINAL_SCHEMA);
    assertTrue(originalClass.isRecord(), "Compiled class should be a record");

    // Create an instance of the original record
    Object originalInstance = createRecordInstance(originalClass, new Object[]{42});
    LOGGER.info("Original instance: " + originalInstance);

    // Serialize the original instance
    byte[] serializedData = serializeRecord(originalInstance);

    // Compile the source code
    // Get the Java compiler
    JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
    if (compiler == null) {
      throw new IllegalStateException("Java compiler not available. Make sure you're running with JDK.");
    }

    // Set up the compilation
    Class<?> evolvedClass = compileAndClassLoad(compiler, FULL_CLASS_NAME, EVOLVED_SCHEMA);
    assertTrue(evolvedClass.isRecord(), "Evolved class should be a record");

    // expected exception
    Exception exception = assertThrows(IllegalArgumentException.class,
        () -> deserializeRecord(evolvedClass, serializedData));

    assertTrue(exception.getMessage().contains("NONE"),
        "Exception should mention NONE compatibility mode");
  }

  @Test
  void testOriginalToEvolvedDeserialization() throws Exception {
    // Set up the compilation
    Class<?> originalClass = compileAndClassLoad(compiler, FULL_CLASS_NAME, ORIGINAL_SCHEMA);
    assertTrue(originalClass.isRecord(), "Compiled class should be a record");

    // Create an instance of the original record
    Object originalInstance = createRecordInstance(originalClass, new Object[]{42});
    LOGGER.info("Original instance: " + originalInstance);

    // Serialize the original instance
    byte[] serializedData = serializeRecord(originalInstance);

    // Set up the compilation
    Class<?> evolvedClass = compileAndClassLoad(compiler, FULL_CLASS_NAME, EVOLVED_SCHEMA);
    assertTrue(evolvedClass.isRecord(), "Evolved class should be a record");

    // Deserialize using the evolved schema
    Object evolvedInstance = deserializeRecord(evolvedClass, serializedData);
    LOGGER.info("Evolved instance: " + evolvedInstance);

    // Verify the fields
    verifyRecordComponents(evolvedInstance, Map.of(
        "value", 42,
        "name", "default",
        "score", 0.0
    ));
  }

  @Test
  void testMultiGenerationEvolutionBackwards() throws Exception {
    // We are testing the latest code can load two prior versions of the code
    Class<?> latestClass = compileAndClassLoad(compiler, FULL_CLASS_NAME, FURTHER_EVOLVED_SCHEMA);
    assertTrue(latestClass.isRecord(), "Latest version class should be a record");

    // Compile and load the original (oldest) version of the record
    Class<?> priorPriorClass = compileAndClassLoad(compiler, FULL_CLASS_NAME, ORIGINAL_SCHEMA);

    // Create and serialize an instance of the original version with number 100
    Object priorPriorInstance = createRecordInstance(priorPriorClass, new Object[]{100});
    byte[] priorPriorData = serializeRecord(priorPriorInstance);

    // Deserialize the prior prior data into the latest version
    Object deserializedPriorPrior = deserializeRecord(latestClass, priorPriorData);
    LOGGER.info("Deserialized prior-prior instance into latest version: " + deserializedPriorPrior);

    // Verify that the deserialized instance of the old record has 100 as value and default values for other fields
    verifyRecordComponents(deserializedPriorPrior, Map.of(
        "value", 100,
        "name", "default",
        "score", 0.0,
        "active", true
        // timestamp is dynamically generated, so we don't verify it
    ));

    // Compile and load the prior version of the record
    Class<?> priorClass = compileAndClassLoad(compiler, FULL_CLASS_NAME, EVOLVED_SCHEMA);
    assertTrue(priorClass.isRecord(), "Prior version class should be a record");

    // Create and serialize an instance of the prior version with number 200
    Object priorInstance = createRecordInstance(priorClass, new Object[]{200, "test", 99.5});
    byte[] priorData = serializeRecord(priorInstance);

    Object deserializedPrior = deserializeRecord(latestClass, priorData);

    verifyRecordComponents(deserializedPrior, Map.of(
        "value", 200,
        "name", "test",
        "score", 99.5,
        "active", true
        // timestamp is dynamically generated, so we don't verify it
    ));
  }

  @Test
  void testRoundTripWithEvolvedSchema() throws Exception {
    // Compile the source code
    // Get the Java compiler
    JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
    if (compiler == null) {
      throw new IllegalStateException("Java compiler not available. Make sure you're running with JDK.");
    }

    // Set up the compilation
    Class<?> evolvedClass = compileAndClassLoad(compiler, FULL_CLASS_NAME, EVOLVED_SCHEMA);

    // Create an instance with all fields specified
    Object originalInstance = createRecordInstance(evolvedClass, new Object[]{123, "test name", 45.67});
    LOGGER.info("Original evolved instance: " + originalInstance);

    // Serialize and then deserialize
    byte[] serializedData = serializeRecord(originalInstance);
    Object deserializedInstance = deserializeRecord(evolvedClass, serializedData);
    LOGGER.info("Deserialized evolved instance: " + deserializedInstance);

    // Verify all fields are preserved
    verifyRecordComponents(deserializedInstance, Map.of(
        "value", 123,
        "name", "test name",
        "score", 45.67
    ));
  }

  @Test
  void testBackwardsCompatibilityDisabledFails() throws Exception {
    // Temporarily disable backwards compatibility
    System.clearProperty(Pickler.COMPATIBILITY_SYSTEM_PROPERTY);

    // Compile the source code
    // Get the Java compiler
    JavaCompiler compiler1 = ToolProvider.getSystemJavaCompiler();
    if (compiler1 == null) {
      throw new IllegalStateException("Java compiler not available. Make sure you're running with JDK.");
    }

    // Set up the compilation
    Class<?> originalClass = compileAndClassLoad(compiler1, FULL_CLASS_NAME, ORIGINAL_SCHEMA);

    // Compile the source code
    // Get the Java compiler
    JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
    if (compiler == null) {
      throw new IllegalStateException("Java compiler not available. Make sure you're running with JDK.");
    }

    // Set up the compilation
    Class<?> evolvedClass = compileAndClassLoad(compiler, FULL_CLASS_NAME, EVOLVED_SCHEMA);

    // Create and serialize an original instance
    Object originalInstance = createRecordInstance(originalClass, new Object[]{42});
    byte[] serializedData = serializeRecord(originalInstance);

    // Attempting to deserialize with the evolved schema should fail when BACKWARDS compatibility is disabled
    Exception exception = assertThrows(IllegalArgumentException.class,
        () -> deserializeRecord(evolvedClass, serializedData));

    LOGGER.info("Expected exception: " + exception.getMessage());
    assertTrue(exception.getMessage().contains("NONE"),
        "Exception should mention NONE compatibility mode");
  }

  final static JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();


  @Test
  void testFieldRenamingWithBackwardsCompatibility() throws Exception {
    // Define schema with renamed field but same structure
    final var renamedFieldSchema = """
        package io.github.simbo1905.simple_pickle.evolution;
        
        /**
         * A record with renamed field but compatible constructor.
         */
        public record SimpleRecord(int renamedValue, String name) {
            /**
             * Compatibility constructor that allows deserializing from original schema.
             */
            public SimpleRecord(int value) {
                this(value, null); // Maps old field name to new field name
            }
        }
        """;

    Class<?> originalClass = compileAndClassLoad(compiler, FULL_CLASS_NAME, ORIGINAL_SCHEMA);
    DiagnosticCollector<JavaFileObject> diagnostics;
    JavaFileObject sourceFile;
    JavaCompiler.CompilationTask task;
    boolean success;

    // Create and serialize an original instance
    Object originalInstance = createRecordInstance(originalClass, new Object[]{42});
    byte[] serializedData = serializeRecord(originalInstance);

    // Set up the compilation
    diagnostics = new DiagnosticCollector<>();
    InMemoryFileManager fileManager = new InMemoryFileManager(
        compiler.getStandardFileManager(diagnostics, null, null));

    // Create the source file object
    sourceFile = new InMemorySourceFile(FULL_CLASS_NAME, renamedFieldSchema);

    // Compile the source
    task = compiler.getTask(
        null, fileManager, diagnostics, null, null, List.of(sourceFile));

    success = task.call();
    if (!success) {
      throwCompilationError(diagnostics);
    }

    // Return the compiled bytecode
    byte[] classBytes = fileManager.getClassBytes(FULL_CLASS_NAME);

    // Load the compiled class
    InMemoryClassLoader classLoader = new InMemoryClassLoader(
        SchemaEvolutionTest.class.getClassLoader(),
        Map.of(FULL_CLASS_NAME, classBytes));

    Class<?> renamedClass = classLoader.loadClass(FULL_CLASS_NAME);

    // Deserialize using the renamed field schema
    Object deserializedInstance = deserializeRecord(renamedClass, serializedData);
    LOGGER.info("Deserialized with renamed field: " + deserializedInstance);

    // Verify the renamed field has the correct value
    verifyRecordComponents(deserializedInstance, Map.of(
        "renamedValue", 42
    ));
  }

  static Class<?> compileAndClassLoad(JavaCompiler compiler, String fullClassName, String code) throws ClassNotFoundException {
    // Set up the compilation
    DiagnosticCollector<JavaFileObject> diagnostics = new DiagnosticCollector<>();
    InMemoryFileManager fileManager1 = new InMemoryFileManager(
        compiler.getStandardFileManager(diagnostics, null, null));

    // Create the source file object
    JavaFileObject sourceFile
        = new InMemorySourceFile(fullClassName, code);

    // Compile the source
    JavaCompiler.CompilationTask task = compiler.getTask(
        null, fileManager1, diagnostics, null, null, List.of(sourceFile));

    boolean success = task.call();
    if (!success) {
      throwCompilationError(diagnostics);
    }

    // Return the compiled bytecode
    byte[] classBytes1 = fileManager1.getClassBytes(fullClassName);

    // Load the compiled class
    InMemoryClassLoader classLoader1 = new InMemoryClassLoader(
        SchemaEvolutionTest.class.getClassLoader(),
        Map.of(fullClassName, classBytes1));

    return classLoader1.loadClass(fullClassName);
  }
}
