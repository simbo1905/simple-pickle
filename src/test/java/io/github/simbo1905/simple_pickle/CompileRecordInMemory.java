package io.github.simbo1905.simple_pickle;

import javax.tools.*;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.RecordComponent;
import java.net.URI;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/// Demonstrates compiling a Java record defined in a String, loading it,
/// and reflectively instantiating it, all within memory.
public final class CompileRecordInMemory {

  /// Fully qualified name of the record to be compiled.
  private static final String RECORD_CLASS_NAME = "com.example.MyInMemoryRecord";

  /// Source code for the record, defined using a text block.
  /// Note the use of a canonical constructor (implicitly generated).
  private static final String SOURCE_CODE = """
      package com.example;
      
      /// A simple record defined entirely within a String.
      /// It holds a description and an integer value.
      public record MyInMemoryRecord(String description, int value) {
          /// Standard toString() is implicitly generated.
      }
      """;

  /// Entry point for the demonstration.
  ///
  /// @param args Command line arguments (unused).
  /// @throws Exception if compilation, class loading, or reflection fails.
  public static void main(final String[] args) throws Exception {

    // --- Step 1: Obtain the System Java Compiler ---
    /// Retrieves the compiler instance provided by the JDK.
    /// Throws an exception if the compiler is not available (e.g., running without a full JDK).
    final var compiler = ToolProvider.getSystemJavaCompiler();
    if (compiler == null) {
      throw new IllegalStateException(
          "System Java Compiler not found. Ensure the application is run with a JDK."
      );
    }

    // --- Step 2: Prepare and Execute In-Memory Compilation ---
    /// Create a representation of the source code string for the compiler.
    final var sourceFile = new StringSourceJavaObject(RECORD_CLASS_NAME, SOURCE_CODE);
    final var compilationUnits = List.of(sourceFile);

    /// Collect diagnostics (errors, warnings) from the compiler.
    final var diagnostics = new DiagnosticCollector<JavaFileObject>();

    /// Use a custom file manager to handle bytecode output in memory.
    /// This prevents writing .class files to disk.
    /// We pass the standard file manager as a delegate for other operations if needed.
    try (final var fileManager = new InMemoryJavaFileManager(
        compiler.getStandardFileManager(diagnostics, null, null))) {

      /// Configure and run the compilation task.
      final var task = compiler.getTask(
          null,            // Writer for standard output (none)
          fileManager,     // Custom file manager for in-memory I/O
          diagnostics,     // Collector for errors/warnings
          null,            // Compiler options (none)
          null,            // Classes for annotation processing (none)
          compilationUnits // The source code object
      );

      /// Execute the compilation.
      final var success = task.call();

      /// Check for compilation errors.
      if (!success) {
        diagnostics.getDiagnostics().forEach(d ->
            System.err.println("Compilation Error: " + d.getMessage(null))
        );
        throw new RuntimeException("Compilation failed!");
      }

      // --- Step 3: Load the Compiled Class ---
      /// Retrieve the compiled bytecode from the custom file manager.
      final var compiledBytes = fileManager.getCompiledClassBytes();
      if (compiledBytes.isEmpty()) {
        throw new RuntimeException("Compilation succeeded but produced no bytecode.");
      }

      /// Create a custom class loader to load the class from the in-memory bytecode.
      /// It delegates to the parent classloader (the application's loader) for other classes.
      final var classLoader = new InMemoryClassLoader(
          CompileRecordInMemory.class.getClassLoader(),
          compiledBytes
      );

      /// Load the compiled record class by its fully qualified name.
      final var loadedClass = classLoader.loadClass(RECORD_CLASS_NAME);

      // --- Step 4: Reflectively Instantiate the Record ---
      /// Verify that the loaded class is indeed a record.
      if (!loadedClass.isRecord()) {
        throw new RuntimeException("Loaded class is not a record: " + loadedClass.getName());
      }

      /// Records have a canonical constructor whose parameters match the record components.
      /// Get the record components to determine the constructor signature.
      final var components = loadedClass.getRecordComponents();
      final var paramTypes = Stream.of(components)
          .map(RecordComponent::getType)
          .toArray(Class<?>[]::new);

      /// Find the canonical constructor using the parameter types derived from components.
      final var constructor = loadedClass.getDeclaredConstructor(paramTypes);

      /// Prepare the arguments matching the constructor's signature.
      final var constructorArgs = new Object[]{"Instantiated From Memory!", 2024};

      /// Create a new instance of the record using the canonical constructor.
      final var recordInstance = constructor.newInstance(constructorArgs);

      /// Output the successfully created instance.
      System.out.println("Successfully created and loaded record instance:");
      System.out.println(recordInstance);

      /// Optionally, access a component accessor method via reflection.
      final var descAccessor = loadedClass.getMethod("description");
      System.out.println("Accessed description component: " + descAccessor.invoke(recordInstance));

    } // try-with-resources ensures fileManager is closed if it were closable (though ours isn't)
  }

  // --- Helper Nested Classes for In-Memory Compilation ---
  // These are static nested classes as they don't need access to instance state
  // of the outer class. Their visibility is default (package-private) according
  // to the style guide.

  /// Represents Java source code provided as a String.
  /// Implements JavaFileObject to be usable by the JavaCompiler API.
  static final class StringSourceJavaObject extends SimpleJavaFileObject {
    private final String code;

    /// Constructs a source object from a class name and source code string.
    /// @param name Fully qualified class name.
    /// @param code The Java source code.
    StringSourceJavaObject(final String name, final String code) {
      super(URI.create("string:///" + name.replace('.', '/') + Kind.SOURCE.extension), Kind.SOURCE);
      this.code = Objects.requireNonNull(code, "code must not be null");
    }

    /// Provides the source code content to the compiler.
    @Override
    public CharSequence getCharContent(final boolean ignoreEncodingErrors) {
      return code;
    }
  }

  /// Represents compiled Java bytecode stored in a byte array output stream.
  /// Implements JavaFileObject for receiving compiler output.
  static final class ByteArrayJavaFileObject extends SimpleJavaFileObject {
    private final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

    /// Constructs a bytecode object associated with a class name.
    /// @param name Fully qualified class name.
    /// @param kind The kind of file (should be Kind.CLASS).
    ByteArrayJavaFileObject(final String name, final Kind kind) {
      super(URI.create("bytes:///" + name.replace('.', '/') + kind.extension), kind);
      if (kind != Kind.CLASS) {
        throw new IllegalArgumentException("Only Kind.CLASS is supported");
      }
    }

    /// Returns the compiled bytecode as a byte array.
    byte[] getBytes() {
      return outputStream.toByteArray();
    }

    /// Provides the output stream where the compiler writes the bytecode.
    @Override
    public OutputStream openOutputStream() {
      return outputStream;
    }
  }

  /// A JavaFileManager that intercepts bytecode output and stores it in memory
  /// using ByteArrayJavaFileObject instances, instead of writing to disk.
  /// Delegates other file operations to a standard file manager.
  static final class InMemoryJavaFileManager extends ForwardingJavaFileManager<JavaFileManager> {
    private final Map<String, ByteArrayJavaFileObject> compiledClasses = new HashMap<>();

    /// Creates an instance wrapping a standard file manager.
    /// @param fileManager The standard file manager to delegate to.
    InMemoryJavaFileManager(final JavaFileManager fileManager) {
      super(Objects.requireNonNull(fileManager, "fileManager must not be null"));
    }

    /// Called by the compiler to get a file object for writing output.
    /// Intercepts requests for CLASS files to use our in-memory byte array object.
    @Override
    public JavaFileObject getJavaFileForOutput(
        final Location location,
        final String className,
        final JavaFileObject.Kind kind,
        final FileObject sibling
    ) throws IOException {
      if (kind == JavaFileObject.Kind.CLASS) {
        final var fileObject = new ByteArrayJavaFileObject(className, kind);
        compiledClasses.put(className, fileObject);
        return fileObject;
      } else {
        /// Delegate to the standard manager for other kinds (e.g., SOURCE_OUTPUT).
        return super.getJavaFileForOutput(location, className, kind, sibling);
      }
    }

    /// Returns a map of compiled class names to their bytecode.
    /// @return An immutable map of class name to byte array.
    Map<String, byte[]> getCompiledClassBytes() {
      return Collections.unmodifiableMap(
          compiledClasses.entrySet().stream()
              .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().getBytes()))
      );
    }

    /// Overridden to prevent the standard manager from potentially creating
    /// its own classloader prematurely. We use our specific InMemoryClassLoader later.
    @Override
    public ClassLoader getClassLoader(final Location location) {
      return null; // Use our custom loader explicitly later.
    }
  }

  /// A ClassLoader capable of loading classes from byte arrays held in memory.
  /// Delegates to a parent classloader for classes not found in its memory store.
  static final class InMemoryClassLoader extends ClassLoader {
    /// Map storing class names and their corresponding bytecode.
    private final Map<String, byte[]> classBytes;

    /// Creates an instance with a parent loader and a map of bytecode.
    /// @param parent The parent classloader (usually the application's loader).
    /// @param classBytes A map where keys are fully qualified class names
    ///                   and values are their bytecode.
    InMemoryClassLoader(final ClassLoader parent, final Map<String, byte[]> classBytes) {
      super(Objects.requireNonNull(parent, "parent classloader must not be null"));
      /// Take a defensive copy of the map.
      this.classBytes = new HashMap<>(Objects.requireNonNull(classBytes, "classBytes map must not be null"));
    }

    /// Finds and loads a class. Called by loadClass after checking the parent.
    /// Looks for the class bytecode in the in-memory map first.
    @Override
    protected Class<?> findClass(final String name) throws ClassNotFoundException {
      final byte[] bytes = classBytes.get(name);
      if (bytes != null) {
        /// Define the class from the bytecode bytes found in the map.
        /// Remove entry after defining to prevent potential redefinition issues if
        /// the loader instance were reused heavily (though unlikely in this example).
        classBytes.remove(name);
        return defineClass(name, bytes, 0, bytes.length);
      }
      /// If not found in our map, delegate back to the parent classloader's findClass.
      return super.findClass(name);
    }
  }
}

