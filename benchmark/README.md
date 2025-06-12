# Benchmark Suite

Micro-benchmarking system for the No Framework Pickler (NFP) serialization library with integrated size calculation and result collection.

## Quick Start

You must increase the version number in the main lib `pom.xml` and do `mvn install -DskipTests > /tmp/nfp-install.log 2>&1 && echo "NFP installed to local repo" || echo "Install failed"` then update the `benchmark/pom.xml` to match the new version before starting a fresh round of benchmarks.

```bash
# Build and run benchmarks
mvn clean verify > /tmp/benchmark-build.log 2>&1 && echo "Build successful" || echo "Build failed"

# Quick test (short iterations)
./run-single-benchmark.sh SimpleWrite

# Full benchmark suite  
./run-benchmark.sh
```

If you do not do that you will not be testing the latest NFP version and will get misleading results.

## Architecture

### New Self-Contained Benchmark Design

Each benchmark class is now self-contained with:

1. **Package-private test data record** - Contains the actual test object
2. **Integrated size calculation** - Before-all setup measures and writes sizes to dated files  
3. **Clean test type names** - `SimpleWrite`, `SimpleRead`, `Paxos` etc (no "Benchmark" suffix)
4. **Direct result loading** - Server finds matching size files automatically

### Data Flow (Simplified)

```
JMH Benchmark → JSON Results (test names) → Server finds latest sizes file → Combined display
          ↓
    @BeforeAll writes sizes-YYYYMMDD_HHMMSS.json (test object sizes)
```

### Example Benchmark Structure

```java
@BenchmarkMode(Mode.Throughput)
public class SimpleWriteBenchmark {
  
  record TestData(String name, int value, long timestamp, boolean active) implements Serializable {}
  
  private static final TestData testObject = new TestData("BenchmarkTest", 42, System.currentTimeMillis(), true);
  
  @BeforeAll
  static void measureSizes() throws Exception {
    // Calculate NFP, JDK, PTB sizes for testObject
    // Write to sizes-YYYYMMDD_HHMMSS.json with test name "SimpleWrite"
  }
  
  @Benchmark public void nfp(Blackhole bh) { /* NFP serialization */ }
  @Benchmark public void jdk(Blackhole bh) { /* JDK serialization */ }  
  @Benchmark public void protobuf(Blackhole bh) { /* PTB serialization */ }
}
```

### Server Intelligence

When loading benchmark results, Server.java:

1. **Reads JMH results file** - Gets test names from `org.sample.SimpleWriteBenchmark.nfp` → `SimpleWrite`
2. **Finds latest sizes file** - `sizes-YYYYMMDD_HHMMSS.json` (newest timestamp)
3. **Matches test names** - Links `SimpleWrite` results with `SimpleWrite` sizes
4. **Displays combined data** - Performance + size in unified table

### File Outputs

**Per benchmark run:**
- `jmh-result-YYYYMMDD_HHMMSS.json` - Raw JMH performance data
- `sizes-YYYYMMDD_HHMMSS.json` - Test object sizes (written once per test session)

**Size file format:**
```json
{
  "SimpleWrite": {"NFP": 71, "JDK": 1026, "PTB": 32},
  "SimpleRead": {"NFP": 71, "JDK": 1026, "PTB": 32}, 
  "Paxos": {"NFP": 89, "JDK": 1690, "PTB": 45}
}
```

## Benchmark Categories

### Clean Test Type Names
- **SimpleWrite** - Write-only performance (no deserialization)
- **SimpleRead** - Read-only performance (pre-serialized data)
- **SimpleRoundTrip** - Full serialize + deserialize cycle
- **Paxos** - Real distributed consensus protocol messages
- **Array**, **List**, **Map**, etc - Data structure specific tests

### Framework Variants (per test type)
- **nfp** - No Framework Pickler 
- **jdk** - Java built-in serialization
- **protobuf** - Protocol Buffers (where applicable)

## Benefits of New Design

### Self-Contained Tests
- Each benchmark owns its test data
- Size calculation happens automatically in @BeforeAll
- No external size lookup dependencies
- Test data lives with the test code

### Simplified Server Logic  
- No complex JSON conversion chain
- Direct mapping: test name → size data
- Latest sizes file strategy (sizes rarely change)
- Automatic discovery of available test types

### Clean Result Format
- Test types group related framework variants
- UI shows: `SimpleWrite` with NFP/JDK/PTB results (not separate entries)
- Size data always matches performance data (same test session)

### Development Efficiency
- Add new benchmark = automatic size calculation
- No manual size registration required
- Server adapts to new test types automatically
- Dated files prevent overwriting historical data

## File Structure

```
benchmark/
├── src/main/java/org/sample/
│   ├── SimpleWriteBenchmark.java     # Write-only test with integrated sizes
│   ├── SimpleReadBenchmark.java      # Read-only test with integrated sizes  
│   ├── SimpleRoundTripBenchmark.java # Full cycle test with integrated sizes
│   ├── PaxosBenchmark.java           # Protocol test with integrated sizes
│   ├── Source.java                   # NFP/JDK/PTB enum
│   └── JsonResultsGenerator.java     # JMH JSON → NJSON converter
├── sizes-YYYYMMDD_HHMMSS.json       # Test object sizes (per session)
├── jmh-result-YYYYMMDD_HHMMSS.json  # JMH performance results
└── Server.java                       # Self-documenting HTTP server
```

## Development Workflow

### Adding New Benchmark

1. **Create benchmark class** with clean name (e.g., `ArrayBenchmark.java`)
2. **Add test data record** as package-private record
3. **Implement @BeforeAll** size calculation writing to dated sizes file
4. **Add framework methods** `nfp()`, `jdk()`, `protobuf()` as appropriate
5. **Run benchmark** - sizes and results automatically captured
6. **Server discovers** new test type automatically

### No Manual Registration Required
- No central registry to update
- No hardcoded size mappings  
- No external configuration files
- Server adapts to whatever tests exist

This design eliminates the complex chain of JSON parsing, size lookup, and format conversion while making the system more maintainable and self-documenting.
