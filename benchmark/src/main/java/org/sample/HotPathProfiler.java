package org.sample;

import io.github.simbo1905.no.framework.Pickler;
import java.nio.ByteBuffer;
import java.util.logging.Logger;
import java.util.logging.Level;
import java.util.logging.ConsoleHandler;

/// Profile hot path with detailed logging to find performance bottlenecks
public class HotPathProfiler {

    public record TestRecord(String name, int value, long timestamp, boolean active) {}

    public static void main(String[] args) throws Exception {
        // Set up FINEST logging
        Logger logger = Logger.getLogger("io.github.simbo1905.no.framework");
        logger.setLevel(Level.FINEST);
        ConsoleHandler handler = new ConsoleHandler();
        handler.setLevel(Level.FINEST);
        logger.addHandler(handler);
        logger.setUseParentHandlers(false);
        
        final var testData = new TestRecord("BenchmarkTest", 42, System.currentTimeMillis(), true);
        
        System.out.println("=== Hot Path Profiling ===");
        System.out.println("Test data: " + testData);
        
        // Create pickler and buffer
        final var nfpPickler = Pickler.of(TestRecord.class);
        final var nfpBuffer = ByteBuffer.allocate(nfpPickler.maxSizeOf(testData));
        
        System.out.println("\n=== SINGLE SERIALIZATION WITH FINEST TRACING ===");
        nfpBuffer.clear();
        nfpPickler.serialize(nfpBuffer, testData);
        
        System.out.println("\n=== TIMING 1000 SERIALIZATIONS ===");
        long startTime = System.nanoTime();
        for (int i = 0; i < 1000; i++) {
            nfpBuffer.clear();
            nfpPickler.serialize(nfpBuffer, testData);
        }
        long endTime = System.nanoTime();
        
        double durationMs = (endTime - startTime) / 1_000_000.0;
        double opsPerSec = 1000.0 / (durationMs / 1000.0);
        
        System.out.printf("NFP: %.0f ops/s (1000 iterations in %.2f ms)%n", opsPerSec, durationMs);
    }
}