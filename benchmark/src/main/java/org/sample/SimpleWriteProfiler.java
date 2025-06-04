package org.sample;

import io.github.simbo1905.no.framework.Pickler;
import java.io.*;
import java.nio.ByteBuffer;

/// Profile SimpleWrite specifically to find the performance bug
/// Run with: java -XX:StartFlightRecording=filename=simple-write.jfr,duration=15s SimpleWriteProfiler
public class SimpleWriteProfiler {

    public record TestRecord(String name, int value, long timestamp, boolean active) implements Serializable {}

    public static void main(String[] args) throws Exception {
        final var testData = new TestRecord("BenchmarkTest", 42, System.currentTimeMillis(), true);
        
        System.out.println("=== SimpleWrite Profiling ===");
        System.out.println("Test data: " + testData);
        
        // Pre-allocate like the benchmark does
        final var nfpPickler = Pickler.of(TestRecord.class);
        final var nfpBuffer = ByteBuffer.allocate(nfpPickler.maxSizeOf(testData));
        final var jdkOutputStream = new ByteArrayOutputStream();
        
        System.out.println("NFP buffer size: " + nfpBuffer.capacity());
        
        // Warm up
        warmUp(nfpPickler, nfpBuffer, jdkOutputStream, testData);
        
        // Profile NFP writes
        System.out.println("Profiling NFP writes...");
        profileNfpWrites(nfpPickler, nfpBuffer, testData);
        
        // Profile JDK writes  
        System.out.println("Profiling JDK writes...");
        profileJdkWrites(jdkOutputStream, testData);
    }
    
    private static void warmUp(Pickler<TestRecord> pickler, ByteBuffer buffer, ByteArrayOutputStream stream, TestRecord data) throws Exception {
        for (int i = 0; i < 10000; i++) {
            // NFP warmup
            buffer.clear();
            pickler.serialize(buffer, data);
            
            // JDK warmup
            stream.reset();
            try (ObjectOutputStream oos = new ObjectOutputStream(stream)) {
                oos.writeObject(data);
            }
        }
        System.out.println("Warmup complete");
    }
    
    private static void profileNfpWrites(Pickler<TestRecord> pickler, ByteBuffer buffer, TestRecord data) throws Exception {
        final long startTime = System.currentTimeMillis();
        final long endTime = startTime + 5_000; // 5 seconds
        int iterations = 0;
        
        while (System.currentTimeMillis() < endTime) {
            buffer.clear();
            pickler.serialize(buffer, data);
            iterations++;
        }
        
        final long duration = System.currentTimeMillis() - startTime;
        System.out.printf("NFP: %.0f ops/s (%d iterations in %d ms)%n", 
                         (double) iterations / (duration / 1000.0), iterations, duration);
    }
    
    private static void profileJdkWrites(ByteArrayOutputStream stream, TestRecord data) throws Exception {
        final long startTime = System.currentTimeMillis();
        final long endTime = startTime + 5_000; // 5 seconds
        int iterations = 0;
        
        while (System.currentTimeMillis() < endTime) {
            stream.reset();
            try (ObjectOutputStream oos = new ObjectOutputStream(stream)) {
                oos.writeObject(data);
            }
            iterations++;
        }
        
        final long duration = System.currentTimeMillis() - startTime;
        System.out.printf("JDK: %.0f ops/s (%d iterations in %d ms)%n", 
                         (double) iterations / (duration / 1000.0), iterations, duration);
    }
}