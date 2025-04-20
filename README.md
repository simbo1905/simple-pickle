# Simple Pickler

Simple Pickler is a lightweight Java serialization library that dynamically generates serializers for Java records to support type-safe message protocols avoiding reflection. It works with:

- Records containing primitive types or String
- Optional of primitive types or String
- Arrays (including primitive arrays, object arrays, and nested arrays)
- Nested records that only contain the above type 
- Sealed interfaces with record implementations that only contain the above types

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
// Define a simple record
record Person(String name, int age) {
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

### Working with Sealed Interfaces

```java
// Define a sealed interface hierarchy
sealed interface Shape permits Circle, Rectangle {
}

record Circle(double radius) implements Shape {
}

record Rectangle(double width, double height) implements Shape {
}

// Get a pickler for the sealed interface
Pickler<Shape> pickler = Pickler.picklerForSealedTrait(Shape.class);

// Serialize a specific implementation
Shape circle = new Circle(5.0);
ByteBuffer buffer = ByteBuffer.allocate(1024);
pickler.serialize(circle, buffer);
buffer.flip();

// Deserialize back to the correct implementation
Shape deserializedShape = pickler.deserialize(buffer);
// deserializedShape will be a Circle instance
```

### Complex Nested Structures

```java
// Define nested records
record Address(String street, String city, String zipCode) {
}

record Employee(String id, Person person, Address address) {
}

// Create a nested structure
Person person = new Person("John Doe", 35);
Address address = new Address("123 Main St", "Any Town", "12345");
Employee employee = new Employee("E12345", person, address);

// Get a pickler for the Employee record
Pickler<Employee> pickler = Pickler.picklerForRecord(Employee.class);

// Serialize and deserialize
ByteBuffer buffer = ByteBuffer.allocate(1024);
pickler.serialize(employee, buffer);
buffer.flip();

Employee deserializedEmployee = pickler.deserialize(buffer);
```

## License

Apache-2.0
