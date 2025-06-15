# No Framework Pickler Architecture

## Project Overview

**No Framework Pickler** is a lightweight, zero-dependency Java 21+ serialization library that generates type-safe, compact and fast, serializers for records containing value-like types as well as container-like types such as arrays, lists, optionals, and maps of value-like types through **Abstract Syntax Tree (AST) construction**, **staged metaprogramming**, and **static type analysis**.

## Core Concept

The library performs **multi-stage programming** where compilation is divided into a series of intermediate phases, enabling typesafe run-time code generation without reflection on the hot path. The user provides classes and the system builds a pickler that has array tables used for serialization and deserialization. Picklers are aggressively cached so that this work is done once typically at startup time. The pickler is then used to serialize and deserialize objects of the user `record` types that contain lots of internal structure that may be heavily nested.

Naive implementations would use reflection on the hot path and `switch` statements to dispatch to the correct serialization logic. This would be slow and not scale well. We use **static semantic analysis** that constructs `switch` statements at the **meta-stage** (construction time), writing specific lambdas into arrays for every component. These lambdas are "typed" in that they know whether a component is `Map<Enum,List<String>[]>` or any other sophisticated generic type structure through **type environment** (Γ) preservation. There is then no need to use a switch statement on the hot path - we can build specific lambdas to do each of the write, read and size operations.

The **static semantic analysis** phase creates an **Abstract Syntax Tree (AST)** representation for each component using **recursive descent parsing** of Java's Type hierarchy. This works because only method return types preserve their generic signatures at runtime, providing the **typing context** necessary for AST construction.

The AST enables **compile-time specialization** where specific user types are resolved. There are no built-in `Enum<>` types so the `ENUM` is one of possibly several discovered user types. When deserializing we must instantiate the specific user type, so we use the `TagWithType` nodes which close over that type in generated lambdas such that (once again) there is no need to use a `switch` statement or `Map.get` on the hot path.

Note that a specific example might have a list of five `TagWithType` nodes yet in the simple case we have only one. We could have twenty if users have crazily complex nested container types for a component. We never use fixed offsets into such lists. We recursively delegate and only use relative offsets. Containers may (or may not) need to chain to adjacent types in the list. The `MAP` has to delegate to two adjacent lambdas as it is a container over two inner types. So the list is arbitrarily long and we first reverse it as the more specific `TagWithType` nodes are to the right of the container types. We then build specific lambdas for the value types (leaf types) and then build up a chain of lambdas that delegate to the inner lambdas.

If we have a trivial component that is only `double` we have a list of one `TagWithType` which is `[DOUBLE]` and the type is `double.class`. The delegation chain is then a single lambda that writes/reads/sizes the double value. If we have a more complex component such as `List<Optional<Person[]>>` there is a longer specific chain of lambdas that are built. This implements **staged computation** where early stages generate specialized code for later stages.

## Philosophy

The JDK has [Project Valhalla](https://openjdk.org/projects/valhalla/) and "Project Valhalla is augmenting the Java object model with value objects, combining the abstractions of object-oriented programming with the performance characteristics of simple primitives." In future JDKs when more Valhalla JEPs land we will be able to add support for user types that are `value` objects. At this point in time only `Record` is the existing value-like type when used in a way that has conventions that honour the intent that it be a pure data carrier even though it is a reference type. Users of `record` can currently abuse its nature to make it not at all like a value type. In the future there will be language support to give compile time and runtime ensured value-like semantics to any user types. Yet as-at Java 21, the only value-like user types are `Record` and `Enum` types. Our library will attempt to validate at construction of the pickler that the user types are used in a value-like manner as far as we can (application unit tests must do the rest).

As at Java 21 this framework has a number of JDK types such as `String`, `Integer`, `UUID`, etc. that are value-like types. This library supplies code to marshal and unmarshal the state of these value-like objects to the wire. The original contribution of No Framework Pickler is to generate fast logic that uses unreflected direct method handles to serialize and deserialize user-defined `Record` types through **AST-guided metaprogramming**. The intention of this library in the future is to support future JDK value-like types and user value-like types as they are introduced via future Valhalla JEPs.

## Unified Pickler Architecture

### Primary Components

- `Pickler.java` - Main interface with `of(Class<?>)` factory method
- `PicklerImpl.java` - Single unified pickler handling all reachable types with global lookup tables
- `MetaStage.java` - **Static semantic analysis** engine performing **AST construction**
- `TypeStructureAST.java` - **Abstract Syntax Tree** representation of Java generic type hierarchies
- `Constants.java` - Type markers and Tag enum for wire protocol
- `ZigZagEncoding.java` - Compact integer encoding for ordinals

### Core Design

- Single `PicklerImpl<T>` performs **multi-stage programming** where all **static semantic analysis** and type discovery occurs at construction time (**meta-stage**), creating optimal logic using direct method handles without reflection on the hot path (**object-stage**) or switching over values, as the switching is done as metaprogramming at construction time where types known via **typing context** analysis allow switches to create lambdas that know the types that will be encountered at runtime.
- Array-based O(1) operations replace the majority of Map lookups on hot path. Only one map of user classes to their index in an array remains for a single lookup on the write path. On the read path the index into the array can be used as a jump to the pre-constructed lambdas that were built to handle specific user types. We might expect a tiny number of user types in the remaining map such that the lookup will be fast. Yet in an adversarial case with 100s of user types the map lookup is far better than a linear search over the array of `userTypes`. The map lookup is O(1) and the linear search is O(n).
- Ordinal-based wire protocol with 1-indexed logical ordinals (0=NULL) for built-in types and positive numbers as array indexes for the user types. These will be small integers that are ZigZag encoded and will normally only take one byte to encode.

The net effect is that the **static semantic analysis** and zigzag encoding means that as long as there are fewer than 63 user types in any sealed hierarchy, a single byte is used to encode the types rather than having to encode the entire user type class name. Then rather than doing `Class.forName` at runtime we have lambdas that invoke the unreflected constructor.

### No-Reflection Principle

*Core Design Philosophy:* The library avoids reflection on the hot path for performance.
All reflective operations are done once during `Pickler.forClass(Class<?>)` construction to build method handles and global lookup tables through **AST construction**.

**Meta-Stage (Construction Time)**:
- **Type Discovery**: Exhaustively discover ALL reachable types from root class using **recursive descent parsing**
- **AST Construction**: Build **Abstract Syntax Trees** for each component's generic type structure using `MetaStage.analyze()`
- **Type Environment**: Build global lookup tables indexed by ordinal for O(1) operations, preserving **typing context** (Γ)
- **Method Handle Caching**: Cache method handles for record constructors and field accessors using `MetaStage.forRecord()`
- **Stable Ordering**: Sort discovered classes lexicographically for stable ordinals
- **Static Analysis**: Create `TypeStructureAST` for generic components
    - Example: `UserRecord[]` → AST: `[TagWithType(ARRAY, Arrays.class), TagWithType(RECORD, UserRecord.class)]`
    - Pre-resolve all user type ordinals and close over them in writers/readers/sizers
    - Eliminates need for runtime type inspection or map lookups on hot path (except one case explained below)
- **Tag vs Constants Distinction**: `StructuralTag` enum represents logical types at **static semantic analysis** (OPTIONAL, ARRAY, RECORD). `Constants` enum represents runtime wire markers (OPTIONAL_EMPTY, OPTIONAL_OF, specific ordinals)
- **Collections Writer Pattern**: Lists, Maps and Arrays have boxed types and delegate to a chained writer per element
- **Collections Arrays Pattern**: Arrays can optimise _some_ but not all writes:
    - `byte[]` is directly written as `ByteBuffer#put(bytes)`
    - `boolean[]` is packed as a `BitSet` then written as `ByteBuffer#put(bytes)`
    - `int[]`/`long[]` arrays are sampled for up to 32 elements to estimate if ZigZagEncoding will save space. If so they are written as varint/varlong and if not they are written as `putInt` or `putLong` leaf nodes in delegation chains
    - `Record[]` and `Enum[]` are written as RECORD or ENUM markers and then elements are written using the record or enum writer

**Object-Stage (Runtime)**:
- Execute pre-built delegation chains without type inspection
- Use cached method handles for optimal performance
- Leverage **staging context** for specialized serialization paths

### Wire Protocol Design

- Negative ordinals (-1, -2, -3...): Built-in types (int, String, etc.)
- Positive ordinals (1, 2, 3...): User types (1-indexed logical, 0-indexed physical)
- Zero (0): NULL marker for memory safety

**Wire Protocol Implementation**:
- **Negative markers**: Built-in types (STRING, UUID, primitives) use negative ZigZag encoded values
- **Positive markers**: User types use array index as ZigZag encoded ordinal
- **NULL marker**: Remains 0 (uninitialized memory safety)
- **No Class Name Serialization**: Ordinals eliminate need for class name compression
- Our built-in value types in `Constants` have a `marker()` method that returns `-1*ordinal()` which is their position in the enum's logical `values()` array yet we do not need to use that physical method as we use the `ordinal()` API
- The user types are stored in the `discoveredClasses` array and we use `classToOrdinal(clazz)` to get the ordinal of the class. We can then `+1` to not overlap with the built-in ordinals
- We ZigZag encode the ordinals which are typically tiny so typically only one byte on the wire. This is a vast performance improvement over writing class names as strings

We will likely never put in more than 63 future built-in types and the application will likely never have more than 63 user types for one pickler so we will get a one byte encoding. Yet we have no such artificial limit - we can support up to 2^31 types of each. If we only had two or three user types then scanning `Class<?>[] userTypes` to do an equals will be optimal. That might even be the typical case - my most complex protocol that I encode only has a half dozen user types. Yet we may face adversarial cases with 100s of user types. In that case the map lookup is O(1) and the linear search is O(n). The JVM has specific logic for small maps to change their behaviour as more elements are added. We can then lean upon the JVM to optimise the map lookup for both the typical small map case and the adversarial large map case.

### Wire Protocol: 1-Indexed Logical Ordinals

**Safety First Design**:
- `0` = NULL marker (safe for uninitialized memory buffers)
- Negative numbers (-1, -2, etc.) = Built-in types (int, String, etc.)
- **Positive numbers (1, 2, 3, etc.) = User types with 1-indexed logical ordinals**

**Ordinal Mapping**:
- **Logical ordinal 1** → **Physical array index 0** (first discovered type)
- **Logical ordinal 2** → **Physical array index 1** (second discovered type)
- etc.

**Memory Safety Benefit**: Accidental zero-filled buffers become `null` instead of crashing on invalid array access.

### Serialization Strategy

**Top-Level Objects**:
1. Write logical ordinal (1, 2, 3, ...) using `Constants.marker()` for built-in types or `classToOrdinal(clazz) + 1` for user types which will correctly write `-1 * ordinal` for built-in types and `index + 1` for user types
2. Based on AST tag, serialize type-specific content:
    - **ENUM**: Write `enum.ordinal()`
    - **RECORD**: Recursively serialize all components via `serializeRecordComponents()`

**Record Components**:
- Built-in types: Write negative marker + value
- User types: Write logical ordinal + recursive content
- NULL values: Write 0 marker

### Deserialization Strategy

**Ordinal-to-Type Lookup**:
1. Read logical ordinal from buffer
2. Convert to physical array index: `physicalIndex = logicalOrdinal - 1`
3. Look up type: `discoveredClasses[physicalIndex]`
4. Dispatch based on AST tag: `tags[physicalIndex]`

**Type-Specific Deserialization**:
- **ENUM**: Read constant name, use `Enum.valueOf()`
- **RECORD**: Read all components, invoke constructor via `MethodHandle`

### Abstract Syntax Tree (AST) Construction and Formal Grammar

The **static semantic analysis** implements **recursive descent parsing** of Java's Type hierarchy to construct **Abstract Syntax Trees** that represent the nested container structure of parameterized types. This AST construction enables **compile-time specialization** through **multi-stage programming**.

#### Formal EBNF Grammar

The AST construction produces type structures conforming to the following formal grammar:

```ebnf
TypeStructure ::= TagTypeSequence

TagTypeSequence ::= TagWithType { TagWithType }

TagWithType ::= Tag Type

Tag ::= ContainerTag | PrimitiveTag

ContainerTag ::= 'ARRAY' | 'LIST' | 'OPTIONAL' | 'MAP'

PrimitiveTag ::= 'BOOLEAN' | 'BYTE' | 'SHORT' | 'CHARACTER' 
               | 'INTEGER' | 'LONG' | 'FLOAT' | 'DOUBLE' 
               | 'STRING' | 'UUID' | 'ENUM' | 'RECORD' | 'INTERFACE'

Type ::= PrimitiveType | ContainerType

PrimitiveType ::= JavaPrimitive | UserDefinedType

JavaPrimitive ::= 'boolean.class' | 'byte.class' | 'short.class' | 'char.class'
                | 'int.class' | 'long.class' | 'float.class' | 'double.class'
                | 'String.class' | 'UUID.class'

UserDefinedType ::= EnumClass | RecordClass | InterfaceClass

ContainerType ::= 'Arrays.class' | 'List.class' | 'Map.class' | 'Optional.class'
```

#### Type Pattern Grammar for Nested Structures

For recursive nesting patterns:

```ebnf
TypePattern ::= SimplePattern | MapPattern

SimplePattern ::= { SimpleContainer } PrimitiveTag

MapPattern ::= { SimpleContainer } 'MAP' TypePattern ',' TypePattern

SimpleContainer ::= 'ARRAY' | 'LIST' | 'OPTIONAL'
```

#### AST Construction Algorithm

The **recursive descent parser** implements the following algorithm:

1. **Container Recognition**: Identifies parameterized types (List<T>, Map<K,V>, Optional<T>) and arrays (T[])
2. **Recursive Decomposition**: Recursively analyzes type arguments to arbitrary depth using **typing context** preservation
3. **AST Construction**: Builds parallel sequences of structural tags and concrete types maintaining **type environment** (Γ)
4. **Termination**: Reaches leaf nodes at primitive/user-defined types

**Example Analysis**: `List<Map<String, Optional<Integer[]>[]>>`

```
Parse Tree Construction:
1. List<T> → T = Map<String, Optional<Integer[]>[]>
   AST: [TagWithType(LIST, List.class), ...]

2. Map<K,V> → K = String, V = Optional<Integer[]>[]  
   AST: [TagWithType(LIST, List.class), TagWithType(MAP, Map.class), ...]
   
3. Key analysis: String → STRING
   AST: [LIST, MAP, STRING, MAP_SEPARATOR, ...]
   
4. Value analysis: Optional<Integer[]>[]
   4a. Array: T[] → T = Optional<Integer[]>
       AST: [LIST, MAP, STRING, MAP_SEPARATOR, ARRAY, ...]
   4b. Optional<T> → T = Integer[]  
       AST: [LIST, MAP, STRING, MAP_SEPARATOR, ARRAY, OPTIONAL, ...]
   4c. Array: T[] → T = Integer
       AST: [LIST, MAP, STRING, MAP_SEPARATOR, ARRAY, OPTIONAL, ARRAY, ...]
   4d. Primitive: Integer
       AST: [LIST, MAP, STRING, MAP_SEPARATOR, ARRAY, OPTIONAL, ARRAY, INTEGER]

Final TypeStructureAST:
tagTypes: [TagWithType(LIST, List.class), TagWithType(MAP, Map.class), 
          TagWithType(STRING, String.class), TagWithType(MAP_SEPARATOR, null),
          TagWithType(ARRAY, Arrays.class), TagWithType(OPTIONAL, Optional.class), 
          TagWithType(ARRAY, Arrays.class), TagWithType(INTEGER, Integer.class)]
```

#### Delegation Chain Construction

The AST enables **right-to-left metaprogramming**:
- **Leaf-first construction**: Build primitive type handlers first
- **Container wrapping**: Wrap primitive handlers with container logic
- **Delegation chains**: Each container delegates to its component handlers
- **Compile-time optimization**: Eliminate all runtime type inspection

### Chaining Writers and Readers

The **static semantic analysis** always has containers on the left, of any depth, and the leaf types on the right. This means we walk from right→left to create inner→outer delegation chains where outer writers delegate to inner writers. What we delegate to must exist before the thing that delegates from it. So we construct right→left and chain them to run left→right→left. The outer writers write their type marker and then delegate. This means that the wire markers reflect the AST structure:

`List<Optional<Person[]>>` →
`[LIST, OPTIONAL, ARRAY, RECORD]` →
`[-1*Constants.LIST.ordinal(), -1*Constants.OPTIONAL.ordinal(), -1*Constants.ARRAY.ordinal(), 1 + classToOrdinal(Person.class)]`

It is then clear that the reader chain will have to read this in wire order to know that it has to go in reverse order. We first run through the outer readers which will do null checks and only if they should not use null will delegate to the inner readers. This will walk down the `[LIST, OPTIONAL, ARRAY, RECORD]` and it will be the innermost container, in this case ARRAY, that will read the length and invoke the specific record reader for many `Person` instances.

We should note that `[ARRAY, OPTIONAL, LIST, PERSON]` has absolutely no difference. We can have `[ARRAY, OPTIONAL, LIST, LIST, OPTIONAL, ARRAY, PERSON]` or whatever. We are simply doing the same **staged metaprogramming** pattern.

**Wire Protocol Encoding**:
- **Built-in types** (String, Integer, etc.): Write negative index (e.g., `-1 * Constants.STRING.ordinal()`)
- **User types** (Records, Enums): Write positive index into discovered types array (1-indexed: `ordinal + 1`)

**Deserialization Order**: Must materialize leaf-first (inside-out) - you cannot create a container until its contents exist. The reader chain naturally enforces this by reading the deepest elements first.

**Why This Architecture**: The AST abstraction allows uniform handling despite Java's syntax differences between arrays (postfix) and generics (prefix). The parallel `TagWithType` lists enable O(1) lookups during **meta-stage programming** without runtime type inspection.

### Performance Characteristics

**O(1) Operations**:
- **Type lookup**: Direct array indexing replaces `Map#get` overhead for almost all cases except one small case on the write path
- **Method invocation**: Pre-cached MethodHandles, no reflection on hot path through **AST-guided specialization**
- **Memory layout**: Compact arrays, excellent cache locality

**Minimal Wire Overhead**:
- Single ordinal per user type (1 byte for first 127 types)
- No class name serialization or compression needed
- Optimal encoding for common cases

### Backwards/Forwards Compatibility Strategy

In the older generation architecture we wrote class names and enum names so that in the future new types can be ignored or skipped over. In the new architecture if the user adds a new Enum or Record user type then the ordinal will be set by lexicographical ordering on the class names. This will be a breaking change. Yet backwards and forwards compatibility is opt-in. Some users will never need this. It is solved in things like protocol buffers by managing ordering via a mapping file which is a .proto file. If a user needs to support backwards and forwards compatibility then they must give us an ordinal mapping where they have not put in a breaking change of adding a new class at an ordinal of an existing class. The application user can give us a different ordinal map of classes and we can validate that it is complete. We can provide diagnostic tools to make it easier to get that correct or an easier 'migrations tool' or such. We do not need to solve that now as it is a migration path between old and new user code that can be made easier yet we need peak performance for users who are not opting into that feature as it is unnecessary for them.

### Buffer Allocation and maxSizeOf Strategy

**The maxSizeOf method is an optional hot path**. While serialize/deserialize are always on the hot path, maxSizeOf usage depends on the application's buffer allocation strategy:

1. **Fixed Buffer Pools**: Applications with predictable message sizes can pre-allocate fixed-size buffers and reuse them. These applications never call maxSizeOf.

2. **Dynamic Allocation**: Applications with variable message sizes (following power-law distributions where most messages are <1KB but occasional "monster" messages exist) benefit from maxSizeOf to avoid:
    - Pre-allocating monster buffers for all messages (wastes memory, increases GC pressure)
    - Buffer overflow exceptions from undersized allocations

3. **Performance Consideration**: When maxSizeOf is used, it's called for EVERY message before serialization. Therefore, any HashMap lookups in maxSizeOf must be hoisted into the pre-computed structures just like serialize/deserialize paths through **AST-guided optimization**.

### Critical Design Principle: Runtime Type Only

**The unified pickler has no special "root type" logic.** During serialization/deserialization:
- Always use `object.getClass()` to get the concrete runtime type
- Look up the concrete type's ordinal in the discovery arrays built during **meta-stage analysis**
- Never use the "root class" passed to the constructor for serialization logic
- Abstract sealed interfaces never appear as concrete instances on the hot path

**Design vs Implementation Errors:**
- **Design Error (Omission)**: Missing specification in documentation
- **Implementation Error (Commission)**: Adding code not specified by design (e.g., unnecessary `rootOrdinal` field)
- **When design and implementation match**: Code will be minimal and fast

### Array Type Casting Solution

**Design Insight**: The unified pickler has global lookup tables indexed by ordinal built through **AST construction** - use them!

**Problem**: Array deserialization needs component type but was trying to peek at elements or read class names.

**Performance Issue**: It is crazy to loop over an array of bytes and write out each byte one at a time with a byte marker. We write out the marker for the byte once when writing the array then do `buffer.put(bytes)` to write the whole byte array in one go. We pack a `boolean[]` as a BitSet that we convert to a `byte[]` for writing. This means that 8 booleans are packed into one byte. The only thing that we just delegate the writing are the user types of RECORD and ENUM.

**Solution**

1. The **AST construction** for types will give us a tag list for `Optional<List<byte[]>>>` resulting in a `TypeStructureAST` with `TagWithType` nodes: `[TagWithType(OPTIONAL, Optional.class), TagWithType(LIST, List.class), TagWithType(ARRAY, Arrays.class), TagWithType(BYTE, byte.class)]`.
2. The write chain is built right to left which is inner leaf node to outer container types. In the above example the ordering is `byte[]`, `Arrays.class`, `List.class`, `Optional.class`. The leaf node is the `byte[]` which is written as a byte array. The next outer node is the `Arrays.class` which is written as an ARRAY marker. The next outer node is the `List.class` which is written as a LIST marker. The final outer node is the `Optional.class` which is written as an OPTIONAL marker.

3. **Typed Array Creation**: `Array.newInstance(componentType, length)` using the resolved class from the **type environment**
4. **Element Deserialization**: The ARRAY_WRITER has logic to explicitly optimise how an array is written. If it is a `byte[]` we just `buffer.put(bytes)` - it would be insane to loop over every byte to write out the byte marker and the byte. Clearly to read it back we need to know the number elements to allocate a `byte[]`. The `boolean[]` is converted to a bitset which is converted to `byte[]`. With `int[]` and `long[]` at write time we check the first part of the array to see if it compresses well. If so we write it as INTEGER_ARRAY or LONG_ARRAY. If not we write it as INTEGER or LONG.

**Key Insight**: No need to peek at elements or extend wire protocol - all type information is already available in the global lookup tables by design through **AST-guided type resolution**.

## Recursive Metaprogramming Pattern

No Framework Pickler's ability to handle arbitrarily nested container types is achieved through a **recursive metaprogramming pattern** implemented at construction time through **AST construction** and **static semantic analysis**. This pattern is the key to supporting deeply nested structures without any special-case code or runtime type inspection.

### The Core Pattern: Abstract Syntax Tree Construction

When you create a pickler, the system performs **static semantic analysis** on all reachable types using **recursive descent parsing**. For each component type, it:

1. **Analyzes the type structure** using `MetaStage.analyze()`, which recursively unwraps:
    - Generic types (List<T>, Map<K,V>, Optional<T>)
    - Array types (T[], T[][], etc.)
    - Raw types to their component types

2. **Builds delegation chains** from right-to-left (leaf to outer container) using **AST-guided metaprogramming**:
    - `buildWriterChain()` - Creates serialization lambdas from **TypeStructureAST**
    - `buildReaderChain()` - Creates deserialization lambdas from **TypeStructureAST**
    - `buildSizerChain()` - Creates size computation lambdas from **TypeStructureAST**

### How It Works

Given a complex nested type like `List<Map<String, Optional<Integer[]>[]>>`, the **AST construction** produces:
- **TypeStructureAST** with `TagWithType` nodes: [LIST, MAP, STRING, MAP_SEPARATOR, ARRAY, OPTIONAL, ARRAY, INTEGER]
- Each node represents a layer in the nesting structure with both structural and semantic information

The **staged metaprogramming** then walks this AST from right-to-left:
1. Creates an INTEGER writer/reader/sizer (the leaf)
2. Wraps it in an ARRAY handler
3. Wraps that in an OPTIONAL handler
4. Wraps that in another ARRAY handler (for the Optional[] array)
5. Creates a STRING handler for map keys
6. Combines key and value handlers in a MAP handler
7. Wraps everything in a LIST handler

Each handler knows how to:
- Serialize/deserialize its own structure (e.g., array length, map size)
- Delegate to its component handler(s) for each element

### Runtime Performance

At runtime, there's zero type inspection or switching. The pre-built delegation chains execute as direct method handle invocations through **object-stage specialization**. Whether you have:
- `List<List<List<Double>>>` (deeply nested lists)
- `Map<String, Optional<Record[]>[]>` (mixed containers)
- `Optional<Map<Integer, List<String[]>>>` (complex nesting)

The same **recursive AST pattern** handles them all automatically. The **static semantic analysis** and chain building happen once at construction time (**meta-stage**), resulting in optimal runtime performance with no HashMap lookups or switch statements on the hot path.

### Implementation Details

The key methods that implement this pattern leverage **AST construction**:
- `MetaStage.analyze()` - Performs the **recursive descent parsing** and **AST construction**
- `MetaStage.forRecord()` - Builds **TypeStructureAST** arrays for record components
- `buildWriterChain()` - Builds serialization delegation chain from **AST**
- `buildReaderChain()` - Builds deserialization delegation chain from **AST**
- `buildSizerChain()` - Builds size computation chain from **AST**

Each method uses the **TypeStructureAST** to enable **compile-time specialization** where the **type environment** (Γ) is preserved and used to generate optimal delegation chains without runtime type inspection.

### Next Steps
1. **List immutability**: Return immutable lists from deserialization (current test expectation)
2. **Complete wire protocol consistency**: Implement ZigZag encoding for all markers
3. **Legacy cleanup**: Remove old RecordPickler/SealedPickler classes and replace with **AST-based implementation**
4. **AST Integration**: Replace existing `TypeStructure.analyze()` with `MetaStage.analyze()` for **formal grammar compliance**
5. **Enhanced Type Safety**: Leverage **static semantic analysis** for compile-time validation of serialization support
