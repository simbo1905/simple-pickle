# Benchmark Suite

Micro-benchmarking system for the No Framework Pickler (NFP) serialization library with automated result collection and analysis.

## Quick Start

```bash
# Build benchmark JAR
mvn clean verify

# Quick test (1 fork, 1 warmup, 1 iteration)
python3 run-benchmark.py -q PaxosBenchmark.paxosProtobuf

# Quick test all benchmarks
python3 run-benchmark.py -q

# Full benchmark suite (5 warmups, 5 iterations)
python3 run-benchmark.py

# Run specific benchmarks
python3 run-benchmark.py PaxosBenchmark SimpleBenchmark
```

## Manual JMH Commands (Legacy)

```bash
# Individual benchmark commands
java -jar target/benchmarks.jar PaxosBenchmark.paxosProtobuf -f 1 -wi 1 -i 1
java -jar target/benchmarks.jar PaxosBenchmark
java -jar target/benchmarks.jar SimpleBenchmark
java -jar target/benchmarks.jar TreeBenchmark
```

## Architecture

### Components

1. **JMH Benchmarks** - Performance measurement using Java Microbenchmark Harness
2. **Size Calculator** - Off-hot-path serialization size measurement  
3. **Results Processor** - Converts JMH JSON to structured NJSON format
4. **Python Runner** - Orchestrates benchmark execution and result processing

### Data Flow

```
JMH Benchmark → JSON Results → Size Calculator → NJSON Processing → Timestamped Results
```

### Benchmark Categories

- **PaxosBenchmark** - Real-world distributed consensus protocol messages
- **SimpleBenchmark** - Basic record serialization (round-trip and write-only)
- **TreeBenchmark** - Nested data structures with recursive types

### Serialization Frameworks Tested

- **NFP** - No Framework Pickler (our library)
- **JDK** - Java built-in serialization (baseline)
- **PTB** - Protocol Buffers (industry standard)

## Results Format

### NJSON Output

Each benchmark run creates timestamped files:
- `jmh-result-YYYYMMDD_HHMMSS.json` - Raw JMH output
- `results-YYYYMMDD_HHMMSS.njson` - Processed results for analysis

### NJSON Schema

```json
{
  "benchmark": "org.sample.PaxosBenchmark.paxosProtobuf",
  "src": "PTB",
  "mode": "thrpt", 
  "score": 1048542.75,
  "error": 12345.67,
  "units": "ops/s",
  "size": 86,
  "ts": "2025-06-03T19:31:11.382546",
  "comment": "Automated benchmark run"
}
```

**Fields:**
- `benchmark` - Full JMH benchmark method name
- `src` - Serialization framework: NFP, JDK, PTB
- `mode` - JMH mode (thrpt = throughput)
- `score` - Operations per second
- `error` - Statistical error margin
- `units` - Measurement units
- `size` - Serialized data size in bytes
- `ts` - ISO timestamp
- `comment` - Description of test run

## Performance Baseline

### Current Results (Example)

**Paxos Protocol Serialization:**
- NFP: ~37k ops/s, 65 bytes
- JDK: ~39k ops/s, 1026 bytes  
- PTB: ~1M ops/s, 86 bytes

**Simple Records:**
- NFP: ~224k ops/s (round-trip)
- JDK: ~12k ops/s (round-trip)
- NFP: 18.7x faster than JDK

## Design Principles

### No Hot-Path Size Calculation

Size measurement is performed off the critical path in `SizeCalculator.java` to avoid skewing performance results. Real applications compute sizes once, not per serialization operation.

### Realistic Test Data

Uses actual distributed systems protocol messages (Paxos consensus) rather than artificial test objects to ensure results reflect real-world performance.

### Automated Result Collection

- **No Overwriting** - Timestamped filenames prevent result loss
- **Structured Output** - NJSON format enables automated analysis
- **Complete Metadata** - Includes sizes, timestamps, and test context

### Quick vs Full Testing

- **Quick Mode (`-q`)** - 1 fork, 1 warmup, 1 iteration for rapid feedback
- **Full Mode** - 5 warmups, 5 iterations for statistically valid results

## File Structure

```
benchmark/
├── run-benchmark.py           # Main orchestration script
├── src/main/java/org/sample/
│   ├── PaxosBenchmark.java    # Real-world protocol benchmarks
│   ├── SimpleBenchmark.java   # Basic record serialization
│   ├── TreeBenchmark.java     # Nested data structures
│   ├── SizeCalculator.java    # Off-path size measurement
│   └── ResultsGenerator.java  # Legacy text parser (unused)
├── results-*.njson           # Timestamped benchmark results
├── jmh-result-*.json         # Raw JMH output files
└── target/benchmarks.jar     # Executable JMH benchmark suite
```

## Development Notes

### Adding New Benchmarks

1. Create benchmark class with `@Benchmark` methods
2. Follow naming convention: `methodNameFramework` (e.g., `paxosNfp`, `paxosJdk`, `paxosProtobuf`)
3. Update `SizeCalculator.java` to include size measurements
4. Update Python script source detection logic if needed

### Framework Integration

- **NFP Integration** - Uses `Pickler.of(Class)` + `ByteBuffer` API
- **JDK Integration** - Standard `ObjectOutputStream`/`ObjectInputStream`
- **Protobuf Integration** - Generated proto classes with manual conversion

### Quality Gates

- All benchmarks must compile and run without errors
- Size calculations must be performed off hot-path
- Results must include complete metadata for analysis
- Quick tests must provide rapid feedback for development iteration

## Troubleshooting

### Common Issues

**Compilation Errors:**
```bash
mvn clean compile  # Regenerate protobuf sources
```

**Missing Benchmark JAR:**
```bash
mvn clean verify   # Rebuild shaded JAR
```

**Size Calculator Failures:**
- Check that all required classes are on classpath
- Verify protobuf generation completed successfully
- Ensure NFP library is available in local Maven repo

### Debug Mode

Add verbose logging to Python script or run JMH directly with additional flags:
```bash
java -jar target/benchmarks.jar -h  # List all JMH options
```
