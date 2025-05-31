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

## Implementation Priority:
1. **Fix enum bug** (blocks Paxos and enum testing)
2. **Simple types** (establish baseline)
3. **Collections** (common use cases)  
4. **Complex scenarios** (NFP strengths)
5. **Real-world protocols** (practical validation)

## Deliverables:
- Individual benchmark classes for each category
- Updated SizeCalculator supporting all test data  
- Complete results.njson with all measurements
- Performance analysis identifying NFP strengths/weaknesses
- Recommendations for optimization targets