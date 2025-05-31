# NFP Performance Profiling Guide

## Overview
This document provides comprehensive guidance for profiling No Framework Pickler (NFP) performance issues using Java Flight Recorder (JFR) and other tools.

## Profiling Workflow

### 1. Problem Identification
- Run benchmarks to identify performance regressions
- Compare NFP vs JDK vs Protobuf performance
- Isolate specific operations (read vs write) 
- Quantify performance gaps (e.g., 10x slower write)

### 2. Create Focused Reproducer
- Create minimal test class that isolates the slow path
- Remove benchmark noise and focus on specific operation
- Include warm-up to eliminate JIT compilation effects
- Run for sufficient duration (10+ seconds) for stable profiling

### 3. Profile with JFR
```bash
# Run with JFR recording
java -XX:StartFlightRecording:filename=profile.jfr -cp classpath MainClass

# Analyze CPU hotspots
jfr view hot-methods profile.jfr

# Analyze memory allocation patterns
jfr view allocation-by-site profile.jfr
jfr view allocation-by-class profile.jfr

# Additional useful views
jfr view contention-by-site profile.jfr  # Thread contention
jfr view gc profile.jfr                  # Garbage collection impact
```

### 4. Analysis Methodology
- **hot-methods**: Identify which methods consume most CPU time
- **allocation-by-site**: Find memory allocation bottlenecks  
- **allocation-by-class**: Understand which types are allocated most
- Look for patterns indicating:
  - Reflection on hot path (MethodHandle, Class operations)
  - Excessive object creation
  - String concatenation/logging overhead
  - HashMap resizing or inefficient data structures

### 5. Root Cause Analysis
- Cross-reference high CPU usage with high allocation sites
- Identify if work should be moved from hot path to initialization
- Look for repeated expensive operations that could be cached
- Check for debug code accidentally left enabled

### 6. Create Regression Test
- Add performance test to MachineryTests with minimum acceptable performance
- Include FIXME comment explaining the issue
- Ensure test fails with current performance, passes after fix

### 7. Documentation
- Create profiling report with timestamp: `profiling-report.YYYY-MM-DD_N.txt`
- Document findings, root cause, and recommended fixes
- Update this guide with lessons learned

## JFR Command Reference

### Recording Options
```bash
# Start recording for specific duration
jfr start --name myrecording --duration 60s --filename recording.jfr

# Attach to running process
jcmd <pid> JFR.start name=myrecording duration=60s filename=recording.jfr

# Live analysis without file
jcmd <pid> JFR.view hot-methods
```

### Key Analysis Views
```bash
# CPU Performance
jfr view hot-methods recording.jfr
jfr view compiler-phases recording.jfr

# Memory Analysis  
jfr view allocation-by-site recording.jfr
jfr view allocation-by-class recording.jfr
jfr view gc recording.jfr

# Concurrency Issues
jfr view contention-by-site recording.jfr
jfr view thread-cpu-load recording.jfr

# Method Handle/Reflection Issues
jfr view compilation recording.jfr
jfr view deoptimizations-by-reason recording.jfr
```

### Output Formatting
```bash
# Control table formatting
jfr view --width 120 --cell-height 3 --truncate beginning hot-methods recording.jfr

# Show underlying query (debug)
jfr view --verbose allocation-by-site recording.jfr

# Custom time ranges for live analysis
jcmd <pid> JFR.view maxage=1h maxsize=200MB hot-methods
```

## Common Performance Anti-Patterns

### Reflection on Hot Path
**Symptoms**: High allocation in `Class.getRecordComponents0()`, `MethodHandle` operations
**Fix**: Move reflection work to pickler construction time, cache method handles

### Eager String Building
**Symptoms**: `StringBuilder` allocations, string concatenation in hot methods
**Fix**: Use lambda logging: `LOGGER.fine(() -> "expensive " + computation)`

### HashMap Resizing
**Symptoms**: High allocation in `HashMap.resize()`, `HashMap$Node` arrays
**Fix**: Pre-size HashMaps with known capacity, use more efficient data structures

### Excessive Boxing
**Symptoms**: High allocation of `Integer.valueOf()`, `Double.valueOf()` etc.
**Fix**: Use primitive collections, avoid unnecessary boxing/unboxing

### Debug Code in Production
**Symptoms**: Validation, assertions, or logging calls in hot path
**Fix**: Use conditional compilation or feature flags for debug code

## Benchmark Integration

### Performance Regression Detection
- Add performance tests to MachineryTests for critical paths
- Set minimum acceptable performance thresholds
- Include both read and write operation tests
- Test with realistic data sizes and patterns

### Benchmark Hygiene
- Move allocations off hot path where possible
- Use fair comparison between NFP and JDK (same allocation patterns)
- Separate write vs read vs round-trip measurements
- Pre-allocate test data and buffers in setup methods

## Historical Performance Issues

### Issue #1: Reflection on Write Path (2025-05-31)
- **Problem**: 10x slower write performance vs JDK
- **Root Cause**: Record component reflection happening during serialization
- **Symptoms**: 25% allocation in `LambdaFormEditor`, 20% in `getRecordComponents0()`
- **Fix**: Cache reflection work during pickler construction
- **Report**: `profiling-report.2025-05-31_1.txt`

## Tools and Alternatives

### JFR (Recommended)
- Low overhead (< 1% typically)
- Built into JDK 21+
- Command-line analysis with `jfr view`
- Comprehensive event coverage

### JProfiler/YourKit
- GUI-based profiling
- Good for interactive analysis
- Higher overhead than JFR
- Commercial tools

### Async Profiler
- Low overhead sampling profiler
- Good flame graph generation
- Useful for CPU-intensive analysis
- Open source alternative

## Best Practices

### Profiling Sessions
1. Always warm up JVM before profiling (10k+ iterations)
2. Profile for sufficient duration (10+ seconds)
3. Focus on specific operations, not entire benchmarks  
4. Use realistic data and access patterns
5. Profile in release mode, not debug builds

### Data Interpretation
1. Focus on high-percentage allocation sites first
2. Cross-reference CPU and allocation data
3. Look for patterns, not individual samples
4. Verify fixes with before/after profiling
5. Document findings for future reference

### Performance Culture
1. Add regression tests for fixed issues
2. Profile before major refactoring
3. Benchmark integration points with other systems
4. Monitor performance trends over time
5. Share profiling knowledge with team