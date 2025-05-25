# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

No Framework Pickler is a tiny Java 21+ serialization library that generates type-safe picklers for records and sealed interfaces. The entire implementation is contained in a single Java file (`Pickler.java`) with ~1,100 lines of code.

## Development Commands

### Build and Test
```bash
# Compile and run all tests
mvn clean test

# Run a specific test class
mvn test -Dtest=ClassName

# Build jar
mvn clean package

# Run benchmarks (if needed)
cd benchmark && mvn clean package exec:java
```

### Schema Evolution Testing
The codebase has special system property for testing schema evolution:
```bash
# Test with backwards compatibility
mvn test -Dno.framework.Pickler.Compatibility=BACKWARDS

# Test with forwards compatibility  
mvn test -Dno.framework.Pickler.Compatibility=FORWARDS
```

## Architecture

### Core Components
- **`Pickler<T>` interface**: Main API for serialization/deserialization
- **`RecordPickler<R>`**: Handles record types using MethodHandles for performance
- **`SealedPickler<S>`**: Handles sealed interfaces by delegating to record picklers
- **`Constants` enum**: Type markers for wire protocol (NULL=1, BOOLEAN=2, etc.)
- **`Companion` class**: Static helper methods for serialization/deserialization

### Key Design Patterns
- **Single file architecture**: Everything in `Pickler.java` to avoid dependencies
- **MethodHandle caching**: Avoids reflection on hot path by using `unreflect()` once
- **Exhaustive pattern matching**: Uses sealed interfaces with switch expressions
- **Schema evolution support**: Optional backwards/forwards compatibility via alternative constructors

### Supported Types
The library supports: primitives, String, Optional, Record, Map, List, Enum, Arrays
- All deserialized collections are immutable
- Nested sealed interfaces and records are fully supported
- Binary format uses type markers + data

## Development Guidelines

### Code Style (Critical)
- **MUST follow CODING_STYLE_LLM.md exactly** - use package-private by default, JEP 467 markdown docs (`///`), Records + static methods
- **Use TDD approach** - write failing test first, then implement feature
- **Incremental changes only** - avoid large refactors, add features via new self-contained tests
- **Respect existing patterns** - follow established coding style religiously

### Test Organization
- `src/test/java/io/github/simbo1905/` - same package tests (can test package-private)
- `src/test/java/io/github/simbo1905/no/framework/` - public API tests only
- Test records must be public and often nested inside test classes
- Always create new test classes for new features, don't modify existing tests initially

### Schema Evolution
- Add new record components only at the end
- Provide backwards compatibility constructors that match old parameter order
- Use system property `no.framework.Pickler.Compatibility=BACKWARDS|FORWARDS|ALL` to enable
- Default is strict matching (NONE) for security

## Current Development Task

The codebase needs UUID support added. Requirements:
1. Add `UUID((byte)17, 16, java.util.UUID.class)` to Constants enum
2. Handle UUID serialization using `getLeastSignificantBits()` and `getMostSignificantBits()`
3. Add UUID cases to all switch statements in Companion class
4. Write new test class with public nested record containing UUID field
5. Test round-trip serialization with proper logging

Remember: Write the test first, make it pass, then consider broader changes.