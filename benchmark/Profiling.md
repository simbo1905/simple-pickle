# NFP Performance Profiling Guide

## Problem Identified
NFP write performance is 8x slower than JDK (235k vs 1.9M ops/s). Tree serialization is 5x slower (4.8k vs 24k ops/s).

## Java Flight Recorder (JFR) Profiling

### Token-Efficient Profiling Commands
```bash
# Start JFR recording (redirect output)
jfr start --name myrecording --duration 60s --filename recording.jfr > /tmp/jfr-start.log 2>&1

# Or attach to running process
jcmd <pid> JFR.start name=myrecording duration=60s filename=recording.jfr > /tmp/jfr-attach.log 2>&1

# Analyze results (grep for specific methods)
jfr view hot-methods recording.jfr > /tmp/hot-methods.log
grep -A 5 "org.sample" /tmp/hot-methods.log | head -50

# Check allocations
jfr view allocation-by-site recording.jfr > /tmp/allocations.log
grep -A 5 "ByteBuffer\|HashMap" /tmp/allocations.log | head -50
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

### Likely Culprits for 8x Write Slowdown
1. **Logging Overhead**: Non-lambda logging building expensive strings (check with: `grep "LOGGER.info(\"" *.java`)
2. **Excessive Allocations**: Check for allocations with: `jfr view allocation-by-class recording.jfr | grep -E "HashMap|String\[\]" | head -20`
3. **Method Handle Overhead**: Profile method handle invocations
4. **Missing JIT Optimization**: Ensure warmup is sufficient

### Token-Efficient Profiling Approach
1. **Create minimal reproducer and redirect output:**
```bash
java -cp target/benchmarks.jar org.sample.NfpWriteProfiler > /tmp/nfp-profile.log 2>&1 && echo "Profiling complete" || echo "Profiling failed"
```

2. **Check for completion and errors:**
```bash
tail -20 /tmp/nfp-profile.log
grep -E "Exception|Error" /tmp/nfp-profile.log
```

3. **Analyze JFR output efficiently:**
```bash
# Don't dump entire JFR analysis
jfr view hot-methods nfp-write.jfr > /tmp/jfr-hot.log
# Get top 10 hot methods only
head -30 /tmp/jfr-hot.log | grep -A 1 "%"
```

4. **Compare with JDK baseline:**
```bash
java -cp target/benchmarks.jar org.sample.JdkWriteProfiler > /tmp/jdk-profile.log 2>&1
diff <(grep "ops/s" /tmp/nfp-profile.log) <(grep "ops/s" /tmp/jdk-profile.log)
```

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