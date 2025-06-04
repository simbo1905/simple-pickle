package org.sample;

import io.github.simbo1905.no.framework.Pickler;
import java.io.*;

/// Focused profiler to isolate NFP write path performance issues
/// Run with: java -XX:StartFlightRecording:filename=write-profile.jfr WritePathProfiler
/// Analyze with: jfr view hot-methods write-profile.jfr
public class WritePathProfiler {

    /// Same test data as PrimitiveBenchmark
    public record AllPrimitives(
        boolean boolVal,
        byte byteVal, 
        short shortVal,
        char charVal,
        int intVal,
        long longVal,
        float floatVal,
        double doubleVal
    ) implements Serializable {}

    public static void main(String[] args) throws Exception {
        final var testData = new AllPrimitives(
            true, (byte)42, (short)1000, 'A', 123456, 9876543210L, 3.14f, 2.71828
        );

        System.out.println("=== Write Path Profiling ===");
        System.out.println("Test data: " + testData);
        
        // Warm up JVM
        System.out.println("Warming up...");
        warmUp(testData);
        
        // Profile NFP write operations
        System.out.println("Profiling NFP writes (10 seconds)...");
        profileNfpWrites(testData);
        
        System.out.println("Profiling complete. Analyze with:");
        System.out.println("jfr view hot-methods write-profile.jfr");
        System.out.println("jfr view allocation-by-site write-profile.jfr");
    }
    
    private static void warmUp(AllPrimitives testData) throws Exception {
        final var pickler = Pickler.of(AllPrimitives.class);
        
        // Warm up NFP
        for (int i = 0; i < 10000; i++) {
            var buffer = java.nio.ByteBuffer.allocate(pickler.maxSizeOf(testData));
            pickler.serialize(buffer, testData);
        }
        
        System.out.println("Warmup complete");
    }
    
    private static void profileNfpWrites(AllPrimitives testData) throws Exception {
        final var pickler = Pickler.of(AllPrimitives.class);
        final long startTime = System.currentTimeMillis();
        final long endTime = startTime + 10_000; // Run for 10 seconds
        
        int iterations = 0;
        
        // Tight loop focusing only on write operations
        while (System.currentTimeMillis() < endTime) {
            var buffer = java.nio.ByteBuffer.allocate(pickler.maxSizeOf(testData));
            pickler.serialize(buffer, testData);
            iterations++;
        }
        
        final long actualDuration = System.currentTimeMillis() - startTime;
        final double opsPerSecond = (double) iterations / (actualDuration / 1000.0);
        
        System.out.printf("NFP Write Performance: %.0f ops/s (%d iterations in %d ms)%n", 
                         opsPerSecond, iterations, actualDuration);
    }
}
