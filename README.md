# No Framework Pickler

No Framework Pickler: A tiny, fast, type safe, zero-dependency Java serialization library in a single java source file.  
It creates type-safe, reflection-free serializers for records and sealed interfaces—perfect. It is perfect for building secure, modern message protocols of sealed interfaces. It supports nested records, arrays, maps and simple enum constants. Binary backwards compatibility is enabled through alternative constructors and adding components to the end of the record declaration (see Schema Evolution section below).

It works with nested sealed interfaces of permitted record types or an outer array of such where the records may contain arbitrarily nested:

 - boolean.class
 - byte.class
 - short.class
 - char.class
 - int.class
 - long.class
 - float.class
 - double.class
 - String.class
 - Optional.class
 - Record.class
 - Map.class
 - List.class
 - Enum.class
 - Arrays of the above

When handling sealed interfaces it is requires all permitted subclasses within the sealed hierarchy must be either records or sealed interfaces of records. This allows you to use record patterns with type safe exhaustive switch statements. 

This library if very fast as it avoids reflection on the hot patch by caching MethodHandle which are resolved through reflection when you construct the pickler.  This project is fully functional with 1 Java source file with less than 1,500 lines of code. It creates a single Jar file with no dependencies that is less than 35k in size. 

## Usage

### Basic Record Serialization

```java
/// Define a record using the enum. It **must** be public
public record Month(Season season, String name) {}

/// Define a simple enum with no fields so no custom constructor. It **must** be public
public enum Season { SPRING, SUMMER, FALL, WINTER }

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

// Verify the deserialized enum value
if (!deserializedMonth.equals(december)) {
    throw new AssertionError("should not be reached");
}
```

### Nested Record Tree

```java
/// The sealed interface and all permitted record subclasses must be public
/// Nested sealed interfaces are supported. Yet all concrete types must be records with support components
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
final var originalRoot = new RootNode(internal1, internal2);

// Get a pickler for the TreeNode sealed interface
final var pickler = Pickler.picklerForSealedInterface(TreeNode.class);

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

### Returned Lists Are Immutable

All deserialized list will be immutable: 

```java
/// record must be public
public record ListRecord(List<String> list) {
  // for test purpose there is no canonical constructor that make an immutable copy. 
  // you should use one to avoid any confusion about internal mutability
}

final List<ListRecord> outerList = List.of(new ListRecord(List.of("A", "B")), new ListRecord(List.of("X", "Y")));

// Calculate size and allocate buffer. Limitations of generics means we have shallow copy into an array.
int size = Pickler.sizeOfHomogeneousArray(outerList.toArray(ListRecord[]::new));

ByteBuffer buffer = ByteBuffer.allocate(size);

// Serialize. Limitations of generics means we have to pass the sealed interface class.
Pickler.serializeMany(outerList.toArray(ListRecord[]::new), buffer);

// Flip the buffer to prepare for reading
buffer.flip();

// Deserialize. Limitations of generics means we have to pass the sealed interface class.
final List<ListRecord> deserialized = Pickler.deserialize(ListRecord.class, buffer);

// will throw an exception if you try to modify the outer list of the deserialized record
try {
    deserialized.removeFirst();
    throw new AssertionError("should not be reached");
} catch (UnsupportedOperationException e) {
}
// will throw an exception if you try to modify an inner list inside the deserialized record
try {
  deserialized.forEach(l -> l.list().removeFirst());
  throw new AssertionError("should not be reached");
} catch (UnsupportedOperationException e) {
}
```

###  Returned Maps Are Immutable

Maps within records are returned as immutable.

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
// will throw an exception if you try to modify an inner map inside the deserialized record
try {
    deserialized.relationships().put("brother", new Person("Tom", 35));
    throw new AssertionError("should not be reached");
} catch (UnsupportedOperationException e) {
}
```

### Array of Records Serialization

Arrays are supported. See notes on erase below. 

```java
// Record type must be public
public record Person(String name, int age) {}

// Create an array of Person records
Person[] people = {
    new Person("Alice", 30),
    new Person("Bob", 25),
    new Person("Charlie", 40)
};

// Calculate size and allocate buffer
int size = Pickler.sizeOfMany(people);
ByteBuffer buffer = ByteBuffer.allocate(size);

// Serialize the array
Pickler.serializeMany(people, buffer);
// Prepare buffer for reading
buffer.flip();

// Deserialize the array
List<Person> deserializedPeople = Pickler.deserializeMany(Person.class, buffer);
// Verify the array was properly deserialized
assertEquals(people.length, deserializedPeople.size());

// The elements in the deserialized list should match the original array
IntStream.range(0, people.length)
    .forEach(i -> assertEquals(people[i], deserializedPeople.get(i)));
}

// The returned list is immutable
try {
    deserialized.removeFirst();
    throw new AssertionError("should not be reached");
} catch (UnsupportedOperationException e) {
    }
```

### Complex Nested Sealed Interfaces

This example shows how to serialize and deserialize a heterogeneous array of records that implement a sealed interface. The records are nested within the sealed interface hierarchy, and the serialization process handles the complexity of the nested structure:

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
static Dog dog = new Dog("Buddy", 3);
static Dog dog2 = new Dog("Fido", 2);
static Animal eagle = new Eagle(2.1);
static Penguin penguin = new Penguin(true);
static Alicorn alicorn = new Alicorn("Twilight Sparkle", new String[]{"elements of harmony", "wings of a pegasus"});

static List<Animal> animals = List.of(dog, dog2, eagle, penguin, alicorn);
Pickler<Animal> pickler = Pickler.forSealedInterface(Animal.class);
final var buffer = ByteBuffer.allocate(1024);

// anyone reading back needs to know how many records to read back
animalBuffer.putInt(animals.size());

for (Animal animal : animals) {
    pickler.serialize(animal, buffer);
}

buffer.flip(); // Prepare for reading

// any service reading back needs to know how many records to read back
int size = animalBuffer.getInt();

// Deserialize the correct number of records
List<Animal> deserializedAnimals = new ArrayList<>(size);
IntStream.range(0, size).forEach(i -> {
Animal animal = animalPickler.deserialize(animalBuffer);
  deserializedAnimals.add(animal);
});
```

## Erasure

You will fail fast at compile time if you try this: 

```java
Pickler<Dog> dogPickler = Pickler.forRecord(Dog.class);
// Compile error java: incompatible types: no instance(s) of type variable(s) A exist so that A[] conforms to Dog
dogPickler.serialize(List.of(dog, dog2).stream().toArray(Record[]::new), dogBuffer); 
```

You will fail at runtime if you try this: 

```java
Pickler<Animal> animalPickler = Pickler.forSealedInterface(Animal.class);
// Runtime error Caused by: java.lang.IllegalArgumentException: java.lang.Record is not actually a concrete record class. You may have tried to use a Record[]. 
Pickler.serializeMany(List.of(dog, dog2).toArray(Record[]::new), dogBuffer);
```

That is why you should follow the simple example above of just writing out an int to say how many records you are writing out.
Then the reader should read out that int and then read that many records in a loop. 

## Security

This library is secure by default by:

1. Resolving and caching `MethodHandle`s to the default constructor of records when you create a record; not when you are deserializing.
2. Using the JDK's `ByteBuffer` class to read and write data that is correctly validate by the JDK ByteBuffer methods.
3. Creating strings using the UTF8 encoding using a UTF8 byte array that has been validated by the ByteArray `readUtf8` method.
4. Resolving what are the legal permitted class names of all records within a sealed interface hierarchy at when you create a record; not when you are deserializing.
5. Using the technology that Java has specifically created to model data transfer objects safely which is the `record` types.

The JDK ensures that `record` types can only be constructed bottom-up. This means that the first record to be deserialized is may only be a fully constructed record made from validated primitive types.

When `MethodHandle`s are invoked they validate the types and numbers of parameters then call constructors that must use the canonical constructor else the canonical constructor itself.

If you instantiate a pickler for a `sealed interface` it ensures that the permitted types of the sealed interface are all `record` types else nested `sealed interface`s of records. It then builds a map of the validated classNames to the correct classes. When it reads back the class names this is via the `ByteBuffer` method `readUtf8` which ensures they are valid bytes then it creates the string explicitly using the UTF8 constructor. It then checks that string against the map of permitted class names to classes. Then it delegates to the pickler for the class.

This means that you cannot attack this library to try to get it to deserialize a classes that are not validated record types in the correct type hierarchy with all code be validated and invoked in the correct order as though it was regular Java code not reflective Java code.

## Schema Evolution

While Java Record Pickler supports **opt-in** schema evolution through: 

- **Additive-only schema evolution**: You can add new fields to the end of a record definition.
- **Backward compatibility constructors**: You must add a constructor that matches the older version which means that the order of the fields matches the old code so that when the old code sends an array of components it matches your new constructor.
- **Default values for new fields**: Java will force you to call the default constructor of your new code so it will force you to set the new fields added to the end as `null` or the default value that you want to use.

To enable backward compatibility when adding fields to a record, ensure you define a public constructor that accepts the exact parameter list. This means the number of parameters, order of parameters and types of parameters must match what you old 
code send. They are sent in source code order not by name so you must only add new components to the end of the record definition. As components are written out and read back in based on source code order not by name you can rename your components in the new code.

As the library is secure by default you must opt into this behaviour by setting a system prompt:

```shell
-D=o-framework-pickler.Compatibility=BACKWARDS
```

### Example: Adding a Field to a Record

Consider a scenario with two microservices communicating with each other:

**Original Record (used by older microservice):**
```java
package com.example.protocol;

public record UserInfo(String name, int personalAccessLevel) {
}
```

**Evolved Record (used by newer microservice):**

```java
package com.example.protocol;

/// It is fine to rename components as `MethodHandles` are resolved by position in the code not by source code name
public record UserInfo(String username, int accessLevel, String department) {
    // Default value for backward compatibility
    private static final String DEFAULT_DEPARTMENT_FOR_LEGACY_RECORDS = "UNASSIGNED";
    
    // Backward compatibility constructor
    public UserInfo(String username, int accessLevel) {
        this(username, accessLevel, DEFAULT_DEPARTMENT_FOR_LEGACY_RECORDS);
    }
}
```

### Summary

- Only supports adding new fields at the end of the record definition
- Requires explicit backward compatibility constructors matching the canonical constructor of the old code
- Cannot remove or reorder existing fields
- Cannot change field types
- You **can** change the name of components (`MethodHandle` is resolved by position in source file not by name)
- You can use `null` or set your own default value for new fields in your backward compatibility constructor

### Schema Evolution Testing

To test forwards and backwards compatibility you can write tests that compile java source code for the old and new files
which are demoed in `SchemaEvolutionTest.java` which has public static helpers that you can call yourself. Just drop 
that file into your `src/test/java` directory and use the static methods.

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
| List | 	16         |

The wire protocol is explained in this diagram:

```mermaid
sequenceDiagram
    participant Client
    participant Pickler
    participant ByteBuffer

    %% --- Serialization Phase ---
    Note over Client, ByteBuffer: Serialization Process

    %% 1. Record serialization
    Client->>Pickler: picklerForRecord(recordClass)
    Pickler->>Pickler: (caches method handles for record)
    Client->>Pickler: serialize(record, buffer)
    Pickler->>ByteBuffer: put(RECORD_MARKER)
    Pickler->>ByteBuffer: put(className with deduplication)
    Pickler->>ByteBuffer: put(serialized record components)

    %% 2. Sealed interface serialization
    Client->>Pickler: picklerForSealedInterface(sealedClass)
    Pickler->>Pickler: (caches picklers for all permitted subclasses)
    Client->>Pickler: serialize(sealedObject, buffer)
    Pickler->>ByteBuffer: put(className with deduplication)
    Pickler->>Pickler: get pickler for permitted record
    Pickler->>ByteBuffer: put(serialized record components)

    %% 3. Array serialization
    Client->>Pickler: serializeArray(array, buffer)
    Pickler->>ByteBuffer: put(ARRAY_MARKER)
    Pickler->>ByteBuffer: put(componentType with deduplication)
    Pickler->>ByteBuffer: put(array.length)
    loop For each element in array
        Pickler->>ByteBuffer: put(serialized element)
    end

    %% 4. List serialization
    Client->>Pickler: serializeList(componentType, list, buffer)
    Pickler->>ByteBuffer: put(LIST_MARKER)
    Pickler->>ByteBuffer: put(componentType with deduplication)
    Pickler->>ByteBuffer: put(list.size)
    loop For each element in list
        Pickler->>ByteBuffer: put(serialized element)
    end

    %% --- Deserialization Phase ---
    Note over Client, ByteBuffer: Deserialization Process

    %% 5. Record deserialization
    Client->>Pickler: picklerForRecord(recordClass)
    Client->>Pickler: deserialize(buffer)
    Pickler->>ByteBuffer: get(RECORD_MARKER)
    Pickler->>ByteBuffer: get(className with deduplication)
    Pickler->>Pickler: (finds cached method handles)
    Pickler->>ByteBuffer: get(serialized record components)
    Pickler->>Pickler: invoke constructor (schema evolution aware)
    Pickler->>Client: return record instance

    %% 6. Sealed interface deserialization
    Client->>Pickler: picklerForSealedInterface(sealedClass)
    Client->>Pickler: deserialize(buffer)
    Pickler->>ByteBuffer: get(className with deduplication)
    Pickler->>Pickler: get pickler for permitted record
    Pickler->>ByteBuffer: get(serialized record components)
    Pickler->>Pickler: invoke constructor (schema evolution aware)
    Pickler->>Client: return permitted record instance

    %% 7. Array deserialization
    Client->>Pickler: deserializeArray(componentType, buffer)
    Pickler->>ByteBuffer: get(ARRAY_MARKER)
    Pickler->>ByteBuffer: get(componentType with deduplication)
    Pickler->>ByteBuffer: get(array.length)
    loop For each element in array
        Pickler->>ByteBuffer: get(serialized element)
        Pickler->>Pickler: deserialize element
    end
    Pickler->>Client: return array

    %% 8. List deserialization
    Client->>Pickler: deserializeList(componentType, buffer)
    Pickler->>ByteBuffer: get(LIST_MARKER)
    Pickler->>ByteBuffer: get(componentType with deduplication)
    Pickler->>ByteBuffer: get(list.size)
    loop For each element in list
        Pickler->>ByteBuffer: get(serialized element)
        Pickler->>Pickler: deserialize element
    end
    Pickler->>Client: return
```

Note: The serialization protocol includes an optimization for class names. When a class name is first encountered during serialization, its full name is written to the buffer and its position is memorized. For subsequent occurrences of the same class, only a 4-byte reference to the previous position is written instead of repeating the full class name. This significantly reduces the size of the serialized data when the same classes appear multiple times, such as in arrays or nested structures.

## Why Did Your Write This Framework Killer Code As A Single Java File?

No Framework Pickler came about because I was doing Java Data Oriented programming over sealed traits and I wanted to quickly 
transmit them for a narrow protocol. Doing something quick and dirty in a single Java file felt right. I wanted to try out some 
new java features and avoid reflection and security issues and realized: 

- The Java `record` types is specifically designed to be a safe data transfer object.
- The JDK's `ByteBuffer` class correctly validates UTF8 bytes for Strings and all primitive types.
- The JDK's `MethodHandle` class is a fast and secure way to invoke constructors and methods to access record components without reflection.
- Nested `sealed interfaces` that only contain `records` then we can use exhaustively matched switch statements to deconstruct the records are exactly what you need to model message protocol on Java 21+.

When I looked at just adding a bit more it was easy. Then when I started to write it up to say it missed a lot of feature I took another look around and "state of the art" and saw that there really wasn't much missing. It was fun to add the rest. 

Until now the challenge with using record patterns in switch statements for a pure data exchange protocols are:

- The built-in Java Serialization mechanism is university loathed. Even if was magically fixed in future Java versions no-one will ever trust it
- Drop in replacements for java serialization like [Apache Fury](https://github.com/apache/fury/tree/main/java) at the time of writing only (v0.10.1) under `fure-core/src/main/java` has 229 java source files and 56,000 lines of code. The `fury-core-0.10.1.jar` Jar file is 1.9M in size.
- The historic way to deal with things is to use a "standard" protocol like  JDK Serialization, Protocol Buffers, Apache Avro, JSON, Hessian, Flatbuffers,, Thrift (TBinaryProtocol, TCompactProtocol, TJSONProtocol), MessagePack, XML. That makes you have to learn and deal with the complexities and security vulnerabilities of libraries such as Kryo, Fst, Protostuff, Jsonb, Protobuf, Flatbuffers, Jackson, Thrift, Fury (when it is stable), FastJSON, JBoss Serialization

The fact that you are spoilt for choices is not a good thing. The sorry history shows that the mapping between arbitrary Java types and standard protocols is very hard. 

The answer to avoid all this complexity is to leverage the modern JDK. We can then potentially free thousands of teams from thousands of hours of build time with a no framework solution that replaces entire frameworks in a single Java file. 

## License

SPDX-FileCopyrightText: 2025 Simon Massey
SPDX-License-Identifier: Apache-2.0
