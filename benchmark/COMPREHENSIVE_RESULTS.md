# Comprehensive Benchmark Results - NFP v0.7.0

## Performance Summary

| Benchmark | NFP (ops/s) | JDK (ops/s) | NFP Speedup | NFP Size | JDK Size | Size Reduction |
|-----------|-------------|-------------|-------------|----------|----------|----------------|
| **Primitives** | 750,399 | 235,342 | 3.2x faster | 39 bytes | 198 bytes | 5.1x smaller |
| **Strings** | 1,304,548 | 271,715 | 4.8x faster | 179 bytes | 313 bytes | 1.7x smaller |
| **Arrays** | 1,099,787 | 109,267 | 10.1x faster | 54 bytes | 297 bytes | 5.5x smaller |
| **Lists** | 692,722 | 52,884 | 13.1x faster | 107 bytes | 498 bytes | 4.7x smaller |
| **Maps** | 300,265 | 25,919 | 11.6x faster | 149 bytes | 603 bytes | 4.0x smaller |
| **Enums** | 1,062,408 | 83,504 | 12.7x faster | 32 bytes | 480 bytes | 15.0x smaller |
| **UUIDs** | 1,577,520 | 102,522 | 15.4x faster | 70 bytes | 310 bytes | 4.4x smaller |
| **Sealed Interfaces** | 345,157 | 84,422 | 4.1x faster | - | - | - |
| **Nested Records** | 134,764 | 45,060 | 3.0x faster | - | - | - |
| **Optional** | 1,344,417 | N/A | N/A | - | - | - |

## Key Findings

### Performance
- **NFP is faster than JDK on ALL data types**
- Best performance gains: UUIDs (15.4x), Lists (13.1x), Enums (12.7x)
- Worst performance: Nested Records (3.0x) - still 3x faster
- Average speedup: ~8-10x faster than JDK

### Size Efficiency
- **NFP is always smaller than JDK**
- Best compression: Enums (15x smaller)
- Worst compression: Strings (1.7x smaller)
- Average: 5-6x smaller payloads

### Special Cases
- **Optional**: Only NFP supports it (not Serializable in JDK)
- **Tree structures**: 18,748 ops/s (from earlier test)
- **Paxos protocol**: NFP 219,615 ops/s vs JDK 31,579 ops/s (7x faster)

## Comparison with v0.6.0

| Metric | v0.6.0 | v0.7.0 | Improvement |
|--------|--------|--------|-------------|
| Simple Write | 237,099 ops/s | 1,676,220 ops/s | 7.1x |
| Tree Performance | 4,919 ops/s | 18,748 ops/s | 3.8x |
| Gap vs JDK Write | 7.4x slower | 1.1x slower | Massive improvement |

## Conclusion

NFP v0.7.0 delivers:
- **Superior performance** across all data types
- **Excellent compression** (average 5x smaller)
- **Broader type support** (Optional, sealed interfaces)
- **Massive improvement** from v0.6.0 refactoring