# NFP Systematic Testing Plan

## Token-Efficient Benchmark Testing Loop

### CRITICAL: Follow This Process for All Benchmark Work

1. **Before ANY benchmark run:**
```bash
# Check if JAR exists
ls -la target/benchmarks.jar || echo \"Need to build JAR first\"
```

2. **Run benchmarks with output control:**
```bash
# Quick test with output redirection
python3 run-benchmark.py -q > /tmp/bench.log 2>&1 && echo \"SUCCESS\" || echo \"FAILED: $?\"

# Check results without burning tokens
tail -10 /tmp/bench.log | grep -E \"ops/s|error|NJSON\"
```

3. **When debugging failures:**
```bash
# Layer 1: Check exit code
echo \"Exit code: $?\"

# Layer 2: High-level errors
grep -E \"ERROR|FAILED|Exception\" /tmp/bench.log | head -10

# Layer 3: Specific error context (doubling strategy)
grep -A 5 -B 5 \"NullPointer\" /tmp/bench.log
grep -A 10 -B 10 \"NullPointer\" /tmp/bench.log  # If need more context
```

4. **When analyzing performance:**
```bash
# Extract just the performance numbers
grep \"ops/s\" results-*.njson | tail -20 > /tmp/perf-summary.txt
cat /tmp/perf-summary.txt

# Compare before/after
diff <(grep \"nfpWrite\" results-20250603*.njson | tail -1) \
     <(grep \"nfpWrite\" results-20250609*.njson | tail -1)
```

5. **When adding new benchmarks:**
```bash
# Compile and check for errors
mvn compile > /tmp/compile.log 2>&1 && echo \"Compiled\" || \
  (echo \"Failed\" && grep -A 2 -B 2 \"error:\" /tmp/compile.log)
```

### Why This Matters for Benchmarks
- JMH output is EXTREMELY verbose (100s of lines per benchmark)
- Maven builds generate massive logs
- Profiler output can be 1000s of lines
- Size calculation failures need specific error extraction

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

### Phase 6: Enum Types
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
- **No runtime failures**
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

## Implementation Priority:
1. **Performance microbenchmarks** (measure current performance)
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