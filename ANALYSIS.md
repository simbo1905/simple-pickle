# Analysis of Record Serialization/Deserialization Issue

## Problem Description

`mvn test -Dtest=MachineryTests#testLinkedRecord`

The test `testLinkedRecord` is failing with:

```
java.lang.IllegalStateException: Expected RECORD marker but got: 1
```

The marker value `1` corresponds to `Constants.NULL.marker()` based on the Constants enum.

## Root Cause Analysis

### Writing Side (DELEGATING_RECORD_WRITER)

The `DELEGATING_RECORD_WRITER` in the Writers class:

1. Writes a `Constants.RECORD.marker()`

2. Checks if the class has been seen before and writes either:

    - A negative reference (using bitwise complement) if seen before
    - A positive length + class name if first time

3. Then delegates to the actual record pickler to serialize the record content

### Reading Side (Current Issues)

1. `RECORD_READER` is a placeholder that returns `null` - it's not implemented
2. `recordSameTypeReader` expects `Constants.SAME_TYPE.marker()` but the writer uses `Constants.RECORD.marker()` for nested records
3. There's a mismatch between what's written and what's expected to be read

### Self-Referencing Records Issue

For self-referencing records (like LinkedRecord), the flow should be:

1. First record writes: `RECORD` marker + class info + record content
2. Nested self-references write: `SAME_TYPE` marker + record content (no class info needed)
3. But the current reader logic doesn't handle this properly

## Plan to Fix

### Step 1: Implement DELEGATING_RECORD_READER

- Rename `RECORD_READER` to `DELEGATING_RECORD_READER`
- Implement the logic to:

    1. Read the `RECORD` marker
    2. Read the class reference/name
    3. Resolve the class and get its pickler
    4. Delegate to the pickler for deserialization

### Step 2: Fix Lazy Evaluation

- Change the reader creation to use lazy evaluation: `(buffer) -> DELEGATING_RECORD_READER.apply(buffer)`
- This prevents eager evaluation that could cause issues with recursive types

### Step 3: Improve Logging

- Add more detailed logging to `DELEGATING_RECORD_WRITER` to show:
- What marker is being written
- Whether it's a class reference or new class
- The class name being written
- Add corresponding logging to `DELEGATING_RECORD_READER`

### Step 4: Handle Class Resolution
- Implement the logic to resolve interned class names back to actual classes
- This requires access to the `ReadBufferImpl.nameToClass` map
- May need to pass additional context to the reader functions


### Step 5: Coordinate SAME_TYPE vs RECORD markers

- Ensure `DELEGATING_SAME_TYPE_WRITER` writes `SAME_TYPE` marker
- Ensure `recordSameTypeReader` only handles `SAME_TYPE` marker
- Ensure `DELEGATING_RECORD_READER` only handles `RECORD` marker

## Key Changes Needed

1. **In Readers class:**
    - Rename `RECORD_READER` to `DELEGATING_RECORD_READER`
    - Implement the full record reading logic
    - Make it lazy: `(buffer) -> DELEGATING_RECORD_READER.apply(buffer)`

2. **In Writers class:**
    - Add more detailed logging to `DELEGATING_RECORD_WRITER`

3. **In createLeafReader method:**
 - Update the RECORD case to use the lazy evaluation pattern


4. **Class resolution:**
    - Need to figure out how to access the `ReadBufferImpl.nameToClass` map from the reader functions
    - This might require passing additional context or restructuring the reader creation

## Testing Strategy

- The `testLinkedRecord` should pass once the record reading is properly implemented
- Verify that both self-referencing records and regular nested records work
- Check that class name interning/resolution works correctly


