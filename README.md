# Simple Pickler

Simple Pickler is a lightweight Java serialization library that dynamically generates serializers for Java records to support type-safe message protocols avoiding reflection. It works with:

- Records containing primitive types or String
- Optional of primitive types or String
- Arrays (including primitive arrays, object arrays, and nested arrays)
- Nested records that only contain the above type 
- Sealed interfaces with record implementations that only contain the above types
- Nested sealed interfaces that only contain the above types

The library dynamically generates serializers at runtime, caching them for reuse to avoid redundant creation and infinite recursion.

## Support Types And Their Type Markers

| Type      | Type Marker |
|-----------|---|
| Integer   | 0 |
| Long      | 1 |
| Short     | 2 |
| Byte      | 3 |
| Double    | 4 |
| Float     | 5 |
| Character | 6 |
| Boolean   | 7 |
| String    | 8 |
| Optional  | 9 |
| Record    | 10 |
| null      | 11 |
| Array     | 12 |

## Example Protocol

An example protocol could look like this: :

```java
// Client to server messages
sealed interface Command permits Push, Pop, Peek {}
record Push(String item) implements Command {}
record Pop() implements Command {}
record Peek() implements Command {}
// Server responses
sealed interface Response permits Success, Failure {
  String payload();
}
record Success(Optional<String> value) implements Response {
  public String payload() { return value.orElse(null); }
}
record Failure(String errorMessage) implements Response {
  public String payload() { return errorMessage;}
}
```

You can find a complete test of this protocol in the `testProtocolExample()` method in `PicklerTest.java`, which demonstrates serialization and deserialization of commands and responses, including a simulated client-server interaction.

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

// Serialize to a ByteBuffer
ByteBuffer buffer = ByteBuffer.allocate(1024);
pickler.serialize(person, buffer);
buffer.flip();

// Deserialize from the ByteBuffer
Person deserializedPerson = pickler.deserialize(buffer);
```

### Complex Example Nested Sealed Interfaces

```java
sealed interface Animal permits Mammal, Bird, Alicorn {
}

sealed interface Mammal extends Animal permits Dog, Cat {
}

sealed interface Bird extends Animal permits Eagle, Penguin {
}

public record Alicorn(String name, String[] magicPowers) implements Animal {
}

public record Dog(String name, int age) implements Mammal {
}

public record Cat(String name, boolean purrs) implements Mammal {
}

public record Eagle(double wingspan) implements Bird {
}

record Penguin(boolean canSwim) implements Bird {
}

Dog dog = new Dog("Buddy", 3);
Alicorn alicorn = new Alicorn("Twilight Sparkle", new String[]{"elements of harmony", "wings of a pegasus"});

var dogBuffer = ByteBuffer.allocate(64);
animalPickler.serialize(dog, dogBuffer);
dogBuffer.flip();
var returnedDog = animalPickler.deserialize(dogBuffer);

Alicorn alicorn = new Alicorn("Twilight Sparkle", new String[]{"elements of harmony", "wings of a pegasus"});

var alicornBuffer = ByteBuffer.allocate(256);
animalPickler.serialize(alicorn, alicornBuffer);
alicornBuffer.flip();
var returnedAlicorn = (Alicorn) animalPickler.deserialize(alicornBuffer);
if (Arrays.equals(alicorn.magicPowers(), returnedAlicorn.magicPowers())) {
System.out.println("Alicorn serialized and deserialized correctly");
} else {
    throw new AssertionError("Alicorn serialization failed");
}
```

## License

SPDX-FileCopyrightText: 2025 Simon Massey
SPDX-License-Identifier: Apache-2.0
