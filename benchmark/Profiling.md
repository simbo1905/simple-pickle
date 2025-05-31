# NFP Performance Profiling Guide

## Problem Identified
PrimitiveBenchmark shows NFP write performance is 10x slower than JDK (126k vs 1.3M ops/s), while reads are 11x faster. This indicates a bug in the write path, not an architectural limitation.

## Java Flight Recorder (JFR) Profiling

### Quick Start Commands
```bash
# Start JFR recording for 60 seconds
jfr start --name myrecording --duration 60s --filename recording.jfr

# Or attach to running process
jcmd <pid> JFR.start name=myrecording duration=60s filename=recording.jfr

# Analyze results
jfr view hot-methods recording.jfr
jfr view allocation-by-site recording.jfr
jfr view allocation-by-class recording.jfr
```

### Key Views for Performance Analysis
- **hot-methods**: Shows CPU hotspots
- **allocation-by-site**: Memory allocation bottlenecks
- **allocation-by-class**: Which classes allocate most memory
- **contention-by-site**: Thread contention issues

### JFR with Running Process
```bash
# List Java processes
jcmd

# View live profiling data (no file needed)
jcmd <pid> JFR.view hot-methods
jcmd <pid> JFR.view allocation-by-site

# Set time range for analysis
jcmd <pid> JFR.view maxage=1h maxsize=200MB hot-methods
```

### Output Formatting
```bash
# Control table width and formatting
jfr view --width 120 --cell-height 3 --truncate beginning hot-methods recording.jfr

# Verbose mode shows underlying query
jfr view --verbose allocation-by-site recording.jfr
```

## Investigation Strategy

### Likely Culprits for 10x Write Slowdown
1. **Eager String Building in Logging**: Non-lambda logging that builds expensive strings
2. **Excessive Allocations**: HashMap resizing, unnecessary object creation
3. **Reflection on Hot Path**: Method resolution happening during serialization
4. **Debug Code**: Accidentally enabled debug logging or validation

### Focused Profiling Approach
1. Create minimal reproducer class that isolates write path
2. Run with JFR profiling enabled
3. Analyze hot-methods and allocation-by-site
4. Compare against equivalent JDK serialization code
5. Identify specific bottleneck methods

### Expected Findings
- Should see specific NFP methods consuming disproportionate CPU
- Allocation patterns showing unnecessary object creation
- Potential string concatenation or logging overhead
- HashMap operations that could be optimized

## MachineryTests Enhancement
Need to add primitive serialization performance test to catch regressions:
- Test that isolates write vs read performance
- Benchmark comparison with baseline performance
- Automated detection of performance degradation

## Action Items
1. Create `WritePathProfiler.java` - minimal reproducer
2. Run JFR profiling on write-only operations
3. Analyze allocation and CPU hotspots
4. Add performance regression test to MachineryTests
5. Fix identified bottlenecks
6. Re-run benchmarks to verify improvement