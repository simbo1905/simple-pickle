# No Framework Pickler Architecture

## Project Overview

**No Framework Pickler** is a lightweight, zero-dependency Java 21+ serialization library that generates type-safe, reflection-free serializers for records and sealed interfaces. The entire core library is contained in a single Java source file (~1,300 lines) with no dependencies.

## Philosophy

The JDK has [Project Valhalla](https://openjdk.org/projects/valhalla/) and "Project Valhalla is augmenting the Java object model with value objects, combining the abstractions of object-oriented programming with the performance characteristics of simple primitives." In future JDKs when more Valhalla JEPs land we will be able to add support for user types that are `value` objects. At this point in time only` Record` is the existing value-like type when used in a way that has conventions that honour the intent that it be a pure data carrier even though it is a reference type. Users of `record` can currently abuse its nature to make it not at all like a value type. In the future there will be language support to give compile time and runtime ensured value-like semantics to any user types. Yet as-at Java 21, the only value-like user types are `Record` and `Enum` types. Our library will attempt to validate at construction of the pickler that the user types are used in a value-like manner as far as we can (application unit tests must do the rest). 

As at Java 21 this framework has a fixed list of JDK types such as `String`, `Integer`, `UUID`, etc. that are considered "built-in" value-like types. This library supplies handwritten code to marshal and unmarshal the state of these value-like objects to the wire. The original contribution of No Framework Pickler is to generate fast logic that use unreflected direct method handles to serialize and deserialize user-defined `Record` types. The intention of this library in the future be able to support future JDK value-like types and user value-like types as they are introduced via future Valhalla JEPs. 

Ask not "What Can Valhalla Do For Me?" but "What Can I Do For Valhalla?" by using this library to create a set of type safe and fast value-like serializers for your code today.  

## Unified Pickler Architecture (Current)

### Primary Components

**Unified Pickler Architecture:**
- `Pickler.java` - Main interface with `of(Class<?>)` factory method
- `PicklerImpl.java` - Single unified pickler handling all reachable types with global lookup tables
- `Constants.java` - Type markers and Tag enum for wire protocol
- `ZigZagEncoding.java` - Compact integer encoding for ordinals

**Core Design:**
- Single `PicklerImpl<T>` eliminates RecordPickler/SealedPickler separation
- Array-based O(1) operations replace HashMap lookups on hot path
- Direct ByteBuffer operations without wrapper complexity
- Ordinal-based wire protocol with 1-indexed logical ordinals (0=NULL)

### No-Reflection Principle

**Core Design Philosophy:** The library avoids reflection on the hot path for performance. All reflective operations are done once during `Pickler.of(Class<?>)` construction to build method handles and global lookup tables.

**Meta-Programming Phase (Construction Time):**
- Exhaustively discover ALL reachable types from root class
- Build global lookup tables indexed by ordinal for O(1) operations
- Cache method handles for record constructors and field accessors
- Sort discovered classes lexicographically for stable ordinals
- **TypeStructure Analysis**: Create parallel `tags` and `types` lists for generic components
  - Example: `UserRecord[]` → tags: `[ARRAY, RECORD]`, types: `[Arrays.class, UserRecord.class]`
  - Pre-resolve all user type ordinals and close over them in writers/readers/sizers
  - Eliminates need for runtime type inspection or map lookups on hot path
- **Tag vs Constants Distinction**: `Tag` enum represents logical types at static analysis (OPTIONAL, ARRAY, RECORD). `Constants` enum represents runtime wire markers (OPTIONAL_EMPTY, OPTIONAL_OF, specific ordinals)
- **Array Writer Pattern**: Arrays are leaf nodes in delegation chains. Element type known from TypeStructure tags: user types (RECORD/ENUM) write `userOrdinal + 1`, built-in types write `-1 * Constants.TYPE.ordinal()`. No runtime type checking needed.

**Runtime Phase (Hot Path):**
- Direct array indexing: `discoveredClasses[ordinal]`, `constructors[ordinal]`
- No reflection, no HashMap lookups, no delegation overhead
- Direct ByteBuffer operations without wrapper complexity

**Global Lookup Tables (Final Fields in PicklerImpl):**
```java
final Class<?>[] discoveredClasses;        // User types sorted lexicographically
final Tag[] tags;                          // RECORD, ENUM, etc.
final MethodHandle[] constructors;         // Sparse array for records
final MethodHandle[][] componentAccessors; // Sparse array of accessor arrays
final Map<Class<?>, Integer> classToOrdinal; // Fast reverse lookup
```

**Wire Protocol Design:**
- Negative ordinals (-1, -2, -3...): Built-in types (int, String, etc.)
- Positive ordinals (1, 2, 3...): User types (1-indexed logical, 0-indexed physical)
- Zero (0): NULL marker for memory safety

### Unified Architecture Solution

**Core Concept**: Single PicklerImpl that discovers ALL reachable types upfront and uses array-based ordinals instead of class name strings.

**Key Components:**

1. **Exhaustive Type Discovery**: Combine `Companion.recordClassHierarchy()` + `TypeStructure.analyze()` to find all reachable user types (Records, Enums, Sealed Interfaces)

2. **Deterministic Ordering**: Sort discovered classes lexicographically to create stable ordinals that enable future compatibility features

3. **Array-Based Architecture**: Replace map lookups with direct array indexing:
   - `Class<?>[] discoveredClasses` - lexicographically sorted user types  
   - `Tag[] tags` - corresponding type tags (RECORD, ENUM, etc.)
   - `MethodHandle[] constructors` - sparse array, only populated for records
   - `MethodHandle[][] componentAccessors` - sparse array of accessor arrays

We must have an set of OUTER ARRAYS that contains all these things or all reacahbale types. This is so that ww can use the built-in marker/ordinal to jump to the array indexes that hold all the information. If it is a user type we will also use the index/marker/ordinal to jump to the code. We are building a set of global lookup tables where the index is the logical type marker/ordinal and in any such array it is a fixed offset `xxxx[index]` to resolve exactly what we need. We can then avoid expensive map lookups and string comparisons on the hot path! 

4. **Wire Protocol Simplification**:
   - **Negative markers**: Built-in types (STRING, UUID, primitives) use negative ZigZag encoded values
   - **Positive markers**: User types use array index as ZigZag encoded ordinal
   - **NULL marker**: Remains 0 (uninitialized memory safety)
   - **No Class Name Serialization**: Ordinals eliminate need for class name compression and smart buffer state

We will likely never put in more than 63 future built-in types and the application will likely never have more than 63 user types for one pickler so we will get a one byte encoding. Yet we have no such artificial limit we can support up to 2^31 types of each. 

5. **Performance Improvements**:
   - Array access O(1) vs map lookup overhead
   - Single ZigZag byte for type markers in typical cases (<128 user types)
   - Eliminate class name serialization entirely in normal cases
   - Direct method handle invocation without delegation

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
1. Write logical ordinal (1, 2, 3, ...)
2. Based on tag, serialize type-specific content:
   - **ENUM**: Write constant name (length + UTF-8 bytes)
   - **RECORD**: Recursively serialize all components

**Record Components**: 
- Built-in types: Write negative marker + value
- User types: Write logical ordinal + recursive content
- NULL values: Write 0 marker

**Wire Format Example** (TestEnum.VALUE_C):
```
[1] [7] [V,A,L,U,E,_,C]
 │   │   └─ UTF-8 constant name (7 bytes)
 │   └─ String length (1 byte)  
 └─ Logical ordinal 1 (1 byte)
Total: 9 bytes
```

### Deserialization Strategy

**Ordinal-to-Type Lookup**:
1. Read logical ordinal from buffer
2. Convert to physical array index: `physicalIndex = logicalOrdinal - 1`
3. Look up type: `discoveredClasses[physicalIndex]`
4. Dispatch based on tag: `tags[physicalIndex]`

**Type-Specific Deserialization**:
- **ENUM**: Read constant name, use `Enum.valueOf()`
- **RECORD**: Read all components, invoke constructor via `MethodHandle`

### TypeStructure Container Analysis Pattern

**Container Type Symmetry**: All container types follow the same pattern for tags/types lists to enable uniform processing:

- **Arrays**: `short[]` → tags: `[ARRAY, SHORT]`, types: `[Arrays.class, short.class]`
- **Lists**: `List<String>` → tags: `[LIST, STRING]`, types: `[List.class, String.class]`  
- **Optionals**: `Optional<Integer>` → tags: `[OPTIONAL, INTEGER]`, types: `[Optional.class, Integer.class]`
- **Maps**: `Map<K,V>` → tags: `[MAP, K_TAG, V_TAG]`, types: `[Map.class, K.class, V.class]`

**Design Principle**: Use marker classes (`Arrays.class`, `List.class`, etc.) rather than concrete types (`short[].class`) to avoid needing `isArray()` checks. The tag indicates the container logic, the marker class provides uniform handling.

**Writer Chain Processing**: Processes tags right-to-left (leaf-to-container):
1. Rightmost tag (element) → Create element writer/reader
2. Container tag → Create container logic that uses tag-based dispatch, not delegation

### Static Analysis and Generic Type Resolution

**Critical Java Limitation**: Generic type information is erased at runtime except for method signatures. You cannot analyze `Optional<String>[].class` directly because the `<String>` is lost to type erasure. Only method signatures retain generics.

**Solution**: Generic type information is preserved in record component accessor methods. `RecordComponent.getGenericType()` returns the full parameterized type including all generic parameters.

**Java Type Syntax Differences**:
- **Generics**: Prefix notation `Container<Contained>` (e.g., `List<String>`, `Optional<Integer>`)
- **Arrays**: Postfix notation `Contained[]` (e.g., `String[]`, `Person[]`)
- **Arrays are invariant**: Unlike generics which support variance, arrays always know their component type

**TypeStructure Analysis Process**:

1. **Input**: `Type` from `RecordComponent.getGenericType()` (e.g., `List<Optional<Person[]>>`)
2. **Recursive Unwrapping**: Recursively analyze container types to ANY depth, building parallel lists:
   - Each container type (List, Optional, Array, etc.) adds its tag and marker class
   - Continue unwrapping until reaching a leaf type (primitive, String, user record, etc.)
3. **Output**: Parallel tag/type lists representing the complete nesting structure
4. **Example**: `List<Optional<Person[]>>` → 
   - tags: `[LIST, OPTIONAL, ARRAY, RECORD]`
   - types: `[List.class, Optional.class, Arrays.class, Person.class]`
5. **Writer/Reader Chain Construction**: Process tags right-to-left (innermost to outermost):
   - Start with leaf writer/reader (e.g., RECORD writer for Person)
   - Wrap with ARRAY writer/reader (handles element iteration) 
   - Wrap with OPTIONAL writer/reader (handles empty/present)
   - Wrap with LIST writer/reader (handles collection size/elements)
   - Each layer delegates to the inner writer/reader it wraps

**Chaining writers and Readers**:

The analysis always has containers on the left, of any depth and the leaf types on the right. 
This means we just walk from right→left to create the inner→outer then have the outer writers delegate to the inner writers. What we delegate to must exist before the thing we delegate from. So we construct right→left and chain them to run left->right→left. The outer writers write their type marker and then delegate. This means that the wire markers look like the tags list: 

`List<Optional<Person[]>>` → 
`[LIST, OPTIONAL, ARRAY, RECORD]` → 
`[-1*Constants.LIST.ordinal(), -1*Constants.OPTIONAL.ordinal(), -1*Constants.ARRAY.ordinal(), 1 + classToOrdinal(Person.class)]`

It is then clear that the reader chain will have to read this in wire oder to know that it has to go in the reverse order. We first run through the outer readers which will do null checks and only if they should not use null will delegate to the inner readers. This will walk down the `[LIST, OPTIONAL, ARRAY, RECORD]` and it will be the inner most container, in this case ARRAY that will read the length and invoke the specific record reader for many `Person` instances. 

We should note that `[ARRAY, OPTIONAL, LIST, PERSON]` has absolutely no difference. We can have
`[ARRAY, OPTIONAL, LIST, LIST, OPTIONAL, ARRAY, PERSON]` or whatever. We are simply doing the same meta programming pattern. 

**Wire Protocol Encoding**:
- **Built-in types** (String, Integer, etc.): Write negative index (e.g., `-1 * Constants.STRING.ordinal()`)
- **User types** (Records, Enums): Write positive index into discovered types array (1-indexed: `ordinal + 1`)

**Deserialization Order**: Must materialize leaf-first (inside-out) - you cannot create a container until its contents exist. The reader chain naturally enforces this by reading the deepest elements first.

**Why This Architecture**: The tag abstraction allows uniform handling despite Java's syntax differences between arrays (postfix) and generics (prefix). The parallel tag/type lists enable O(1) lookups during meta-programming without runtime type inspection.

### Performance Characteristics

**O(1) Operations**: 
- Type lookup: Direct array indexing replaces HashMap overhead
- Method invocation: Pre-cached MethodHandles, no reflection on hot path
- Memory layout: Compact arrays, excellent cache locality

**Minimal Wire Overhead**:
- Single ordinal per user type (1 byte for first 127 types)
- No class name serialization or compression needed
- Optimal encoding for common cases

### Backwards/Forwards Compatibility Strategy

In the older generation architecture we wrote class names and enum names so that in the future new types can be ignored or skipped over. In the new architecture if the user adds a new Enum or Record user type then the ordinal will be set by lexicographical ordering on the class names. This will be a breaking change. Yet backwards and forwards compatibility is opt-in. Some users will never need this. It is solved in things like protocol buffers by managing ordering via a mapping file which is a .proto file. If an user needs to support backwards and forwards compatibility then they must give us an ordinal mapping where they have not put in a breaking change of adding a new class at an ordinal of an existing class. The application user can give us a different ordinal map of classes and we can validate that it is complete. We can provide diagnostic tools to make it easier to get that correct or an easier 'migrations tool' or such. We do not need to solve that now as it is a migration path between old and new user code that can be made easier yet we need peak performance for users who are not opting into that feature as it is unnecessary for them. 

### Critical Design Principle: Runtime Type Only

**The unified pickler has no special "root type" logic.** During serialization/deserialization:
- Always use `object.getClass()` to get the concrete runtime type
- Look up the concrete type's ordinal in the discovery arrays
- Never use the "root class" passed to the constructor for serialization logic
- Abstract sealed interfaces never appear as concrete instances on the hot path

**Design vs Implementation Errors:**
- **Design Error (Omission)**: Missing specification in documentation
- **Implementation Error (Commission)**: Adding code not specified by design (e.g., unnecessary `rootOrdinal` field)
- **When design and implementation match**: Code will be minimal and fast

### Array Type Casting Solution

**Design Insight**: The unified pickler has global lookup tables indexed by ordinal - use them!

**Problem**: Array deserialization needs component type but was trying to peek at elements or read class names.

**Performance Issue**: It is crazy to loop over an array of bytes and write out each byte one at a time with a byte marker. We write out the marker for the byte once when writing the array then do buffer.put(bytes) to write the whole byte array in one go. We pack a boolean[] as a BitSet that we convert to a `byte[]` for writing. This means that 8 booleans are packed into one byte. The only thing that we just delegate the writing are the user types of RECORD and ENUM. 

**Solution** 

1. The analysis of types will give us a tag list for Optional<List<byte[]>>> will give us a tag list of `[OPTIONAL, LIST, ARRAY, BYTE]` and a type list of `[Optional.class, List.class, Arrays.class, byte[].class]`. 
2. The write chain is built right to left which is inner leaf node to outer container types. In the above example the ordering is `byte[]`, `Arrays.class`, `List.class`, `Optional.class`. The leaf node is the `byte[]` which is written as a byte array. The next outer node is the `Arrays.class` which is written as an ARRAY marker. The next outer node is the `List.class` which is written as a LIST marker. The final outer node is the `Optional.class` which is written as an OPTIONAL marker.

3. **Typed Array Creation**: `Array.newInstance(componentType, length)` using the resolved class
4. **Element Deserialization**: The ARRAY_WRITER has logic to explicitly optimise how an array is written. If it is a `byte[]` we just `butter.put( bytes )` it would be insane to loop over ever byte to write out the byte marker and the byte. Clearly to read it back we need to know the number elements to allocate a `byte[]`. The `boolean[]` is converted to a bitset which is converted to `byte[]`. With `int[]` and `long[]` at write time we check the first part of the array to see if compresses well. If so we write it as INTEGER_ARRAY or LONG_ARRAY. If not we write it as INTEGER or LONG 

**Key Insight**: No need to peek at elements or extend wire protocol - all type information is already available in the global lookup tables by design.

### Next Steps
1. **List immutability**: Return immutable lists from deserialization (current test expectation)
2. **Complete wire protocol consistency**: Implement ZigZag encoding for all markers
3. **Legacy cleanup**: Remove old RecordPickler/SealedPickler classes
