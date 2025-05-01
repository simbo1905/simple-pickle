# No Framework Pickler

No Framework Pickler: A lightweight, zero-dependency Java serialization library that generates type-safe, reflection-free serializers for records and sealed interfaces—perfect for building secure, modern message protocols of sealed interfaces containing nested records, arrays, maps and simple enum constants. It supports binary backwards compatibility of additive changes through alternative constructors (see Schema Evolution section below).

It avoids excessive reflection when working with objects by caching MethodHandle which are resolved through reflection at the 

It works with nested sealed traits that permit nested simple records of simple types: 

- Records containing primitive types or String
  - Optional of primitive types or String
  - Arrays or primitive arrays such as `byte[]`, object arrays, nested arrays
  - Nested records that only contain the above types
  - Maps with keys or values that are any of the above
  - Sealed interfaces with record implementations that only contain the above
  - Nested sealed interfaces that only contain the above
  - Plain enums (enums without custom fields, constructors, or class bodies)
  - An outer array that contains any of the above

When handling sealed interfaces it is requires all permitted subclasses within the sealed hierarchy must be either records or sealed interfaces of records. Upon initializing the pickler for the outermost sealed interface the logic proactively prepares and caches the necessary picklers for all permitted record in the sealed hierarchy. You get one Pickler to rule them all. 

The above restrictions are broad enough to build a rich message protocol suitable for using with record patterns in switch statements. At the same time these restrictions are narrow enough to be easy to memorize the rules to create a pure message exchange protocol. 

An example protocol could look like this:

```java
// Client to server messages
sealed interface StackCommand permits Push, Pop, Peek {}
record Push(String item) implements StackCommand {}
record Pop() implements StackCommand {}
record Peek() implements StackCommand {}
// Server responses
sealed interface StackResponse permits Success, Failure {
  String payload();
}
record Success(Optional<String> value) implements StackResponse {
  public String payload() { return value.orElse(null); }
}
record Failure(String errorMessage) implements StackResponse {
  public String payload() { return errorMessage;}
}
```

Note that there is deliberately no common interface between the client and server protocols. This means that we would 
create two type-safe picklers, one for each side of the shared protocols. This is a deliberate design choice to avoid 
needing to do unchecked casts when deserializing: 

```java
// Get picklers for the protocol interfaces
Pickler<StackCommand> commandPickler = Pickler.picklerForSealedTrait(StackCommand.class);
Pickler<StackResponse> responsePickler = Pickler.picklerForSealedTrait(StackResponse.class);
```

See the unit tests for many examples of using the library.

## What This Solves And Project Goals

The challenge with using record patterns in switch statements for message protocols are:

- The built-in Java Serialization mechanism is university loathed. Even if was magically fixed in future Java versions no-one will ever trust it
- Drop in replacements for java serialization like [Apache Fury](https://github.com/apache/fury/tree/main/java) is at the time of writing only at alpha version 0.10. Under only  `src/main/java` of `fury-core` there are 229 java source files and a quick line count has 56,000 lines of code. In contrast this project has 1 source file with 1,200 lines of code. The `fury-core-0.10.1.jar` Jar file is 1.9M in size. This project makes a `no-framework-pickler.jar` Jar that is only 31k in size
- Standard formats like Protobuf, Avro or JSON require 3rd party libraries dependencies that often have dependencies and/or a lot bigger surface for potential zero-day security vulnerability
- Java 8 boilerplate programming forces the use of kitchen sink frameworks that use the standard 3rd party libraries which then maximizes to a certainly future critical security vulnerabilities
- Mapping between arbitrary Java types and standard protocols is hard and best solved through annotations and arbitrary code. Yet we can map to Java's built-in "data transfer objects" which are records to get idiomatic Java with "obvious" serialization.

If we use a strong convention of `record` types that are the only permitted types of `sealed interface`s and allow nesting of interfaces and nesting of records we get a complete type safe Java message protocol out-of-the-jdk-box. We can avoid the need for annotations and arbitrary code and the code itself becomes the documentation.

The goals of this codebase is to:

1. Write one piece of stable code that is a single Java source file
2. Never add features only fix any bugs
3. Never have any third party dependencies
4. Be good enough to use for the internal communication between software. Remember perfection is the enemy of good. 
5. Support basic additive change to the message protocol for backwards and forwards compatibility of adjacent versions of microservices.
6. Be simple enough to get an LLM to write the corresponding Rust code that can deserialize Java records into Rust structs. Therefore, rather than explicitly supporting cross-language serialization the library implicitly supports it through simplicity. 

This mean you might find that this single Java file solution is a viable alternative to using gRPC in your application. YMMV and T&Cs apply.

## Security

This library is secure by default by:

1. Resolving and caching `MethodHandle`s to the default constructor of records when you create a record; not when you are deserializing. 
2. Using the JDK's `ByteBuffer` class to read and write data that is correctly validate by the JDK ByteBuffer methods. 
3. Creating strings using the UTF8 encoding using a UTF8 byte array that has been validated by the ByteArray `readUtf8` method.
4. Resolving what are the legal permitted class names of all records within a sealed interface hierarchy at when you create a record; not when you are deserializing.
5. Using the technology that Java has specifically created to model data transfer objects safely which is the `record` types. 

The JDK ensures that `record` types can only be constructed bottom-up. This means that the first record to be deserialized is may only be a fully constructed record made from validated primitive types.

When `MehtodHandle`s are invoked they validate the types and numbers of parameters then call constructors that must use the canonical constructor else the canonical constructor itself. 

If you instantiate a pickler for a `sealed interface` it ensures that the permitted types of the sealed interface are all `record` types else nested `sealed interface`s of records. It then builds a map of the validated classNames to the correct classes. When it reads back the class names this is via the `ByteBuffer` method `readUtf8` which ensures they are valid bytes then it ceats the string explicitly using the UTF8 constructor. It then checks that string against the map of permitted class names to clases. Then it delegates to the pickler for the class.

This means that you cannot attack this library to try to get it to deserialize a classes that are not validated record types in the correct type hierarchy with all code be validated and invoked in the correct order as though it was regular Java code not reflective Java code. 

## Usage Examples

### Basic Record Serialization

```java
/// Define a simple record
/// The constructor must be public so that we can invoke the canonical constructor form the pickler package
public record Person(String name, int age) {
}

// Create an instance
var person = new Person("Alice", 30);

// Get a pickler for the record type
Pickler<Person> pickler = Pickler.picklerForRecord(Person.class);

// Calculate size and allocate buffer
int size = pickler.sizeOf(person);
ByteBuffer buffer = ByteBuffer.allocate(size);

// Serialize to a ByteBuffer
pickler.serialize(person, buffer);
buffer.flip();

// Deserialize from the ByteBuffer
Person deserializedPerson = pickler.deserialize(buffer);
```

### Enum Serialization

The library supports serialization of records containing plain Java enums (enums without custom fields or methods). Here's how to serialize a record with an enum component:

```java
/// Define a simple enum
public enum Season { SPRING, SUMMER, FALL, WINTER }

/// Define a record using the enum
/// Constructor must be public for the pickler
public record Month(Season season, String name) {}

// Create an instance
var december = new Month(Season.WINTER, "December");

// Get a pickler for the record type containing the enum
Pickler<Month> pickler = Pickler.picklerForRecord(Month.class);

// Calculate size and allocate buffer
int size = pickler.sizeOf(december);
ByteBuffer buffer = ByteBuffer.allocate(size);

// Serialize to a ByteBuffer
pickler.serialize(december, buffer);
buffer.flip();

// Deserialize from the ByteBuffer
Month deserializedMonth = pickler.deserialize(buffer);

// Verification (Optional, but good practice)
// assertEquals(december, deserializedMonth);
```

### Array of Records Serialization

```java
// Define a simple record
public record Person(String name, int age) {}

// Create an array of Person records
Person[] people = {
    new Person("Alice", 30),
    new Person("Bob", 25),
    new Person("Charlie", 40)
};

// Calculate size and allocate buffer
int size = pickler.sizeOf(people);
ByteBuffer buffer = ByteBuffer.allocate(size);

// Serialize the array
Pickler.serializeArray(people, buffer);
buffer.flip();

// Deserialize the array
Person[] deserializedPeople = Pickler.deserializeArray(buffer, Person.class);

// Verify the array was properly deserialized
assertEquals(people.length, deserializedPeople.length);
// Use streams to verify each element matches
IntStream.range(0, people.length)
    .forEach(i -> assertEquals(people[i], deserializedPeople[i]));
```

### Maps

If we define a map as `Map<String, Person>` then we run into erasure issues that at runtime 
there is no type information about the map. We only have a raw `Map` type and so we do not get a type-safe pickler. We have to wrap the map into a record that is associated with the map type. We then create a pickler for the record type. In practice at runtime the wrapper adds one byte to say its the outer record type plus the class name. That is low overhead to persist the type information given that Java erasure is a runtime "feature". Note that we only ever write any class name once so if you have a lot of instances of maps to write out the overhead is amortized. Here is an example that is taken from the junit tests:

```java
Person john = new Person("John", 40);
Person michael = new Person("Michael", 65);
Person sarah = new Person("Sarah", 63);

Map<String, Person> familyMap = new HashMap<>();
familyMap.put("father", michael);
familyMap.put("mother", sarah);

record NestedFamilyMapContainer(Person subject, Map<String, Person> relationships) {}

// we need a wrapper to create a concrete type for the pickler due to erase of map types
final var nestedFamilyMapContainer = new NestedFamilyMapContainer(john, familyMap);

final var pickler = picklerForRecord(NestedFamilyMapContainer.class);

// Calculate size and allocate buffer
int size = pickler.sizeOf(nestedFamilyMapContainer);
ByteBuffer buffer = ByteBuffer.allocate(size);
// Serialize
pickler.serialize(nestedFamilyMapContainer, buffer);
buffer.flip();
// Deserialize
NestedFamilyMapContainer deserialized = pickler.deserialize(buffer);
// see the unit test that validates the deserialized map matches the original map. 
```

### Complex Nested Sealed Interfaces

```java
// Protocol
sealed interface Animal permits Mammal, Bird, Alicorn {}
sealed interface Mammal extends Animal permits Dog, Cat { }
sealed interface Bird extends Animal permits Eagle, Penguin {}
public record Alicorn(String name, String[] magicPowers) implements Animal {}
public record Dog(String name, int age) implements Mammal {}
public record Cat(String name, boolean purrs) implements Mammal {}
public record Eagle(double wingspan) implements Bird {}
record Penguin(boolean canSwim) implements Bird {}

// Create instances of all animal types
final var dog = new Dog("Buddy", 3);
final var cat = new Cat("Whiskers", true);
final var eagle = new Eagle(2.1);
final var penguin = new Penguin(true);
final var alicorn = new Alicorn("Twilight Sparkle", new String[]{"elements of harmony", "wings of a pegasus"});

// Create an array of all animals
final var originalAnimals = new Animal[]{dog, cat, eagle, penguin, alicorn};

// Get a pickler for the Animal sealed interface
final var pickler = picklerForSealedTrait(Animal.class);

// Calculate total buffer size needed using streams
final var totalSize = Arrays.stream(originalAnimals)
    .mapToInt(pickler::sizeOf)
    .sum();

// Allocate a single buffer to hold all animals
final var buffer = ByteBuffer.allocate(totalSize);

// Serialize all animals into the buffer using streams
        Arrays.stream(originalAnimals)
            .forEach(animal -> pickler.serialize(animal, buffer));

    // Prepare buffer for reading
    buffer.flip();

// Deserialize all animals from the buffer
final var deserializedAnimals = new Animal[originalAnimals.length];
        Arrays.setAll(deserializedAnimals, i -> pickler.deserialize(buffer));
```

### Trees Record Serialization

```java
/// Internal node that may have left and right children
/// Note the real coded in the unit tests has a null safe equals to compare the nodes with null children
public sealed interface TreeNode permits RootNode, InternalNode, LeafNode {}
public record RootNode(TreeNode left, TreeNode right) implements TreeNode {}
public record InternalNode(String name, TreeNode left, TreeNode right) implements TreeNode {}
public record LeafNode(int value) implements TreeNode {}
final var leaf1 = new LeafNode(42);
final var leaf2 = new LeafNode(99);
final var leaf3 = new LeafNode(123);

// A lob sided tree
final var internal1 = new InternalNode("Branch1", leaf1, leaf2);
final var internal2 = new InternalNode("Branch2", leaf3, null);
final var root = new RootNode(internal1, internal2);

// Get a pickler for the TreeNode sealed interface
final var pickler = picklerForSealedTrait(TreeNode.class);

// Calculate buffer size needed for the whole graph reachable from the root node
final var bufferSize = pickler.sizeOf(originalRoot);

// Allocate a buffer to hold just the root node
final var buffer = ByteBuffer.allocate(bufferSize);

// Serialize only the root node (which should include the entire graph)
pickler.serialize(originalRoot, buffer);

// Prepare buffer for reading
buffer.flip();

// Deserialize the root node (which will reconstruct the entire graph)
final var deserializedRoot = pickler.deserialize(buffer);

// See junit tests that Validates the entire tree structure was properly deserialized
validateTreeStructure(deserializedRoot);
```

## Wire Protocol

Support Types And Their Type Markers

| Type      | Type Marker |
|-----------|-------------|
| null | 	1          |
| Boolean | 	2          |
| Byte | 	 3         |
| Short | 	 4         |
| Character | 	5          |
| Integer | 6           |
| Long | 	 7         |
| Float | 	 8         |
| Double | 	 9         |
| String | 	10         |
| Optional | 	11         |
| Record | 	12         |
| Array | 	13         |
| Map | 	14         |
| Enum | 	15         |

The wire protocol is explained in this diagram: 

```mermaid
sequenceDiagram
    participant Client
    participant Pickler
    participant ByteBuffer
    
    Note over Client, ByteBuffer: Serialization Process
    Client->>Pickler: serialize(object, buffer)
    alt Pickler handles null
        Pickler->>ByteBuffer: put(NULL_MARKER)
    else Pickler for Record
        Pickler->>ByteBuffer: put(RECORD_MARKER)
        Note right of Pickler: Class names are deduplicated
        alt First occurrence of class
            Pickler->>ByteBuffer: put(className.length)
            Pickler->>ByteBuffer: put(className.bytes)
            Note right of Pickler: Position is memorized
        else Repeated class
            Pickler->>ByteBuffer: put(reference to previous position)
        end
        Pickler->>ByteBuffer: put(serialized component data)
    else Pickler for Array
        Pickler->>ByteBuffer: put(ARRAY_MARKER)
        Note right of Pickler: Component type name is deduplicated
        alt First occurrence of component type
            Pickler->>ByteBuffer: put(componentType.length)
            Pickler->>ByteBuffer: put(componentType.bytes)
            Note right of Pickler: Position is memorized
        else Repeated component type
            Pickler->>ByteBuffer: put(reference to previous position)
        end
        Pickler->>ByteBuffer: put(array.length)
        loop For each array element
            Pickler->>ByteBuffer: put(serialized element)
        end
    else Pickler for Outer Array
        Pickler->>Pickler: serializeArray(array, buffer)
        Note right of Pickler: Handles arrays of records or sealed interfaces
        Pickler->>ByteBuffer: put(ARRAY_MARKER)
        Note right of Pickler: Component type name is deduplicated
        Pickler->>ByteBuffer: put(componentType info)
        Pickler->>ByteBuffer: put(array.length)
        loop For each array element
            Pickler->>ByteBuffer: put(serialized element)
        end
    else Pickler for sealed interface
        Note right of Pickler: Class name is deduplicated
        alt First occurrence of class
            Pickler->>ByteBuffer: put(className.length)
            Pickler->>ByteBuffer: put(className.bytes)
            Note right of Pickler: Position is memorized
        else Repeated class
            Pickler->>ByteBuffer: put(reference to previous position)
        end
        Pickler->>ByteBuffer: put(serialized object data)
    end
    
    Note over Client, ByteBuffer: Deserialization Process
    Client->>Pickler: deserialize(buffer)
    alt Read NULL_MARKER
        Pickler->>Client: return null
    else Read ARRAY_MARKER for outer array
        Pickler->>Client: deserializeArray(buffer, componentType)
        Note right of Pickler: Handles class name deduplication
        Pickler->>ByteBuffer: get(componentType info)
        Pickler->>ByteBuffer: get(array.length)
        loop For each array element
            Pickler->>ByteBuffer: get(serialized element)
        end
        Pickler->>Client: return reconstructed array
    else Read normal type
        Note right of Pickler: Handles class name deduplication
        alt Reference to previous class
            Pickler->>ByteBuffer: get(reference position)
            Pickler->>Client: lookup class from position map
        else New class
            Pickler->>ByteBuffer: get(className.length)
            Pickler->>ByteBuffer: get(className.bytes)
            Pickler->>Client: Class.forName(className)
            Note right of Pickler: Position is memorized
        end
        Pickler->>ByteBuffer: get(serialized data)
        Pickler->>Client: return reconstructed object
    end
```

Note: The serialization protocol includes an optimization for class names. When a class name is first encountered during serialization, its full name is written to the buffer and its position is memorized. For subsequent occurrences of the same class, only a 4-byte reference to the previous position is written instead of repeating the full class name. This significantly reduces the size of the serialized data when the same classes appear multiple times, such as in arrays or nested structures.

## Schema Evolution

While Java Record Pickler is primarily designed for type-safe serialization of message protocols with simple data transfer records rather than long-term storage, it does support limited schema evolution to facilitate communication between microservices running different versions of a protocol.

### Supported Evolution Pattern

To enable backward compatibility when adding fields to a record, ensure you define a public constructor that accepts the exact parameter list (number and types) of the previous version. The library supports additive-only schema evolution where new fields are added to the end of a record definition. Pickler will automatically use your compatibility constructor when deserializing data from older versions.

### Example: Adding a Field to a Record

Consider a scenario with two microservices communicating with each other:

**Original Record (used by older microservice):**
```java
package com.example.protocol;

public record UserInfo(String username, int accessLevel) {
}
```

**Evolved Record (used by newer microservice):**
```java
package com.example.protocol;

public record UserInfo(String username, int accessLevel, String department) {
    // Default value for backward compatibility
    private static final String DEFAULT_DEPARTMENT_FOR_LEGACY_RECORDS = "UNASSIGNED";
    
    // Backward compatibility constructor
    public UserInfo(String username, int accessLevel) {
        this(username, accessLevel, DEFAULT_DEPARTMENT_FOR_LEGACY_RECORDS);
    }
}
```

### How It Works

1. When the older microservice sends a serialized `UserInfo` record with just `username` and `accessLevel`, the newer microservice can deserialize it using the evolved `UserInfo` class.

2. The Pickler detects that the serialized data has fewer components than the canonical constructor expects.

3. It then looks for the public constructor you provide that accepts exactly the number and types of components in the serialized data.

4. This backward compatibility constructor is invoked, which supplies the default value for the new `department` field.

### Limitations

- Only supports adding new fields at the end of the record definition
- Requires explicit backward compatibility constructors with appropriate default values
- Cannot remove or reorder existing fields
- Cannot change field types

### Best Practices

1. Always add new fields at the end of record definitions
2. Always provide backward compatibility constructors with meaningful default values
3. Use clearly named constants for default values to document their purpose
4. Consider using `Optional<T>` for new fields that might not have a sensible default
5. Test both serialization directions (old → new and new → old) to ensure compatibility

## Contributing

Users of this library can have LLMs write exhaustive tests of
round-trip serialization and deserialization of their message protocols. If you find a bug send a pull request of the
test and fix that the LLM wrote place.

Please avoid suggesting adding new features. Please do fork the repo and add them to your copy. Do raise an discussion issue to advertise the new feature 
to the wider community. LLMs are powerful so go for it. 


## License

SPDX-FileCopyrightText: 2025 Simon Massey
SPDX-License-Identifier: Apache-2.0
