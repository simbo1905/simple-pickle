# NFP Systematic Testing Plan

## Current Issues
1. **CRITICAL**: NFP enum serialization bug (NullPointerException on NoOperation.NOOP)
2. **Performance**: NFP underperforming JDK on Tree benchmarks (14,792 vs 21,768 ops/s)

## Systematic Data Type Testing

### Phase 1: Primitive Types
- [ ] **PrimitiveBenchmark**: boolean, byte, short, char, int, long, float, double
- [ ] **ArrayPrimitiveBenchmark**: Arrays of primitives (int[], byte[], etc.)

### Phase 2: Boxed Types  
- [ ] **BoxedBenchmark**: Boolean, Byte, Short, Character, Integer, Long, Float, Double
- [ ] **ArrayBoxedBenchmark**: Arrays of boxed primitives

### Phase 3: Core Java Types
- [ ] **StringBenchmark**: String serialization (various sizes)
- [ ] **OptionalBenchmark**: Optional.empty(), Optional.of(value)

### Phase 4: Collections
- [ ] **ListBenchmark**: List<Integer>, List<String>, nested List<List<String>>
- [ ] **MapBenchmark**: Map<String, Integer>, Map<String, Person>, nested maps
- [ ] **ArrayComplexBenchmark**: Arrays of records

### Phase 5: Record Types
- [ ] **SimpleRecordBenchmark**: Single-field, multi-field records
- [ ] **NestedRecordBenchmark**: Records containing other records
- [ ] **RecordWithCollectionsBenchmark**: Records with Lists/Maps

### Phase 6: Enum Types (CRITICAL - Fix enum bug first)
- [ ] **EnumBenchmark**: Simple enums, enums with fields
- [ ] **RecordWithEnumBenchmark**: Records containing enums

### Phase 7: Sealed Hierarchies
- [ ] **SimpleSealedBenchmark**: 2-3 record variants
- [ ] **ComplexSealedBenchmark**: Nested sealed interfaces (Animal example)
- [ ] **ListOfSealedBenchmark**: List<Animal> serialization

### Phase 8: Real-World Scenarios
- [ ] **ProtocolBenchmark**: Realistic message protocols
- [ ] **EventSourcingBenchmark**: Event streams with sealed command hierarchies
- [ ] **DTOBenchmark**: Typical data transfer objects

## Testing Methodology

### For Each Benchmark:
1. **Measure sizes FIRST**: Run SizeCalculator before benchmarks
2. **Create fair buffers**: Use measured sizes + margin for allocation
3. **Test all three**: NFP, JDK, Protobuf serialization
4. **Generate NJSON**: Include size, performance, timestamp, comment
5. **Analysis**: Identify performance patterns and size efficiency

### Success Criteria:
- **NFP >= JDK performance** on most workloads
- **NFP <= JDK size** on all workloads  
- **No runtime failures** (fix enum bug first)
- **Comprehensive coverage** of README-listed types

### Expected Findings:
- **Complex nested structures**: NFP should excel (record patterns)
- **Simple primitives**: JDK might be faster (optimized paths)
- **Protocol messages**: NFP should compete with Protobuf
- **Size efficiency**: NFP should beat JDK consistently

## Performance Microbenchmarks (CRITICAL)

### Hot Path Analysis
- [ ] **Stream vs For-Loop**: Test IntStream operations vs traditional loops with JIT escape analysis
- [ ] **Lambda Allocation**: Measure allocation overhead of lambda expressions in streams  
- [ ] **Boxing/Unboxing**: Profile primitive boxing in IntStream operations
- [ ] **Method Handle Overhead**: Compare method handle invocation vs direct field access

### Map Implementation Microbenchmarks  
- [ ] **HashMap vs Map.of()**: Small map performance (1, 2, 5, 10, 20 user types)
- [ ] **HashMap Capacity**: Default constructor vs optimized initial capacity
- [ ] **Specialized Maps**: Test alternative implementations for classToOrdinal lookup
- [ ] **Array vs Map**: Compare direct array indexing vs small map lookup performance

### Allocation Profiling
- [ ] **JProfiler/Async-profiler**: Identify actual allocation hotspots (not speculation)
- [ ] **Escape Analysis**: Verify JIT optimization of short-lived objects
- [ ] **GC Pressure**: Measure allocation rate and GC overhead

## CRITICAL PERFORMANCE ISSUES FOUND

### ❌ FAKE SUCCESS CLAIMED - CORE ARCHITECTURE NOT IMPLEMENTED
- **NFP is 7x slower than JDK** (300k ops/s vs 2M ops/s)
- **Root cause**: Runtime HashMap lookups instead of pre-built writers array
- **Lines 99-100**: `TODO: Build actual writers and readers using existing TypeStructure.analyze logic`
- **Current slop**: `classToOrdinal.containsKey(componentValue.getClass())` for EVERY component
- **Should be**: Direct array access `writers[componentIndex].accept(buffer, componentValue)`

### What Should Have Been Built
1. **Construction time**: Analyze component types, build specialized writer lambdas
2. **Runtime**: Zero lookups - direct array access to pre-built writers
3. **No HashMap checks** on hot path - all type analysis done upfront

### Disgraceful Pattern
- Claimed "All 52 tests passing!" and "Constants enum refactoring complete!"  
- Left fundamental performance architecture as TODO
- Tests pass because runtime checking works - just catastrophically slow
- Never actually implemented the array-based architecture specified

## Implementation Priority:
1. **Fix enum bug** (blocks Paxos and enum testing) ✅ DONE  
2. **🚨 IMPLEMENT ACTUAL WRITERS/READERS ARRAYS** - remove runtime HashMap slop
3. **Performance microbenchmarks** (validate array-based fixes)
4. **Simple types** (establish baseline)
5. **Collections** (common use cases)  
6. **Complex scenarios** (NFP strengths)
7. **Real-world protocols** (practical validation)

## Deliverables:
- Individual benchmark classes for each category
- Updated SizeCalculator supporting all test data  
- Complete results.njson with all measurements
- Performance analysis identifying NFP strengths/weaknesses
- Recommendations for optimization targets