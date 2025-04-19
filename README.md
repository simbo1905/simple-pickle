# Serialized Pickler Generator  

## Overview  

This tool uses the Java 24 ClassFile API to lazily generates `ByteArray` serialization and deserialization logic for simple record types and sealed traits. It dynamically creates `Pickler` implementations for serializing and deserializing objects. This is faster than using reflection which is an easy way to serialize records that are message protocols or simple value object used with data oriented programming.

## How to Use  

```java
// Pass a list of types. You only need to pass in one sealed trait types. Passing in multiple
// passing in multiple types is only needed if you have multiple sealed traits else unreated record types. 
Map<Class<?>, Pickler<?>> picklers = PicklerGenerator.generatePicklers(Class<?>... types);

// Use the picker map to serialize and deserialize your objects
picklers.get(MyClass1.class).serialize(myObject); 
picklers.get(MyClass1.class).deserialize(bytes);
```

### Example with a Java Record

```java 
// Define a simple record 
record Person(String name, int age) {}  
// Generate a pickler for the Person record 
Map<Class<?>, Pickler<?>> picklers = PicklerGenerator.generatePicklers(Person.class); 
Pickler<Person> personPickler = (Pickler<Person>) picklers.get(Person.class);  
// Serialize a Person object to a byte buffer 
Person john = new Person("John", 30); 
ByteBuffer buffer = ByteBuffer.allocate(1024);
personPickler.serialize(john, buffer); 
buffer.flip(); // Prepare buffer for reading  
// Deserialize from the byte buffer
Person deserializedJohn = personPickler.deserialize(buffer); 
System.out.println(deserializedJohn); // Output: Person[name=John, age=30] ```
```

