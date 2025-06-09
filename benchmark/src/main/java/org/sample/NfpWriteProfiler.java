package org.sample;

import io.github.simbo1905.no.framework.Pickler;
import java.io.*;
import java.nio.ByteBuffer;
import java.util.logging.Logger;

/// Isolated NFP write profiler with detailed logging
/// Run with: java -XX:StartFlightRecording=filename=nfp-write.jfr,duration=15s NfpWriteProfiler
public class NfpWriteProfiler {
    
    private static final Logger LOGGER = Logger.getLogger(NfpWriteProfiler.class.getName());

    public record TestRecord(String name, int value, long timestamp, boolean active) implements Serializable {}

    public static void main(String[] args) throws Exception {
        final var testData = new TestRecord("BenchmarkTest", 42, System.currentTimeMillis(), true);
        
        LOGGER.info("=== NFP Write Profiler ==="); // TODO always use LOGGER.xxx( ()->yyyy )
        LOGGER.info("Test data: " + testData); // TODO always use LOGGER.xxx( ()->yyyy )
        
        // Initialize NFP components
        LOGGER.fine("Creating NFP pickler"); // TODO always use LOGGER.xxx( ()->yyyy )
        final var nfpPickler = Pickler.forClass(TestRecord.class);
        
        LOGGER.fine("Calculating max size"); // TODO always use LOGGER.xxx( ()->yyyy )
        final int maxSize = nfpPickler.maxSizeOf(testData);
        LOGGER.info("NFP max size: " + maxSize + " bytes"); // TODO always use LOGGER.xxx( ()->yyyy )
        
        LOGGER.fine("Allocating buffer"); // TODO always use LOGGER.xxx( ()->yyyy )
        final var nfpBuffer = ByteBuffer.allocate(maxSize);
        
        // Test serialization once to verify
        LOGGER.fine("Testing serialization"); // TODO always use LOGGER.xxx( ()->yyyy )
        nfpBuffer.clear();
        int actualSize = nfpPickler.serialize(nfpBuffer, testData);
        LOGGER.info("Actual serialized size: " + actualSize + " bytes"); // TODO always use LOGGER.xxx( ()->yyyy )
        
        // Test deserialization
        nfpBuffer.flip();
        TestRecord deserialized = nfpPickler.deserialize(nfpBuffer);
        LOGGER.info("Deserialized: " + deserialized); // TODO always use LOGGER.xxx( ()->yyyy )
        LOGGER.info("Round-trip successful: " + testData.equals(deserialized)); // TODO always use LOGGER.xxx( ()->yyyy )
        
        // Warm up
        LOGGER.info("Starting warmup"); // TODO always use LOGGER.xxx( ()->yyyy )
        warmUp(nfpPickler, nfpBuffer, testData);
        
        // Profile writes
        LOGGER.info("Starting write profiling (10 seconds)"); // TODO always use LOGGER.xxx( ()->yyyy )
        profileWrites(nfpPickler, nfpBuffer, testData);
        
        LOGGER.info("Profiling complete. Analyze with:"); // TODO always use LOGGER.xxx( ()->yyyy )
        LOGGER.info("jfr view hot-methods nfp-write.jfr"); // TODO always use LOGGER.xxx( ()->yyyy )
        LOGGER.info("jfr view allocation-by-site nfp-write.jfr"); // TODO always use LOGGER.xxx( ()->yyyy )
    }
    
    private static void warmUp(Pickler<TestRecord> pickler, ByteBuffer buffer, TestRecord data) throws Exception {
        LOGGER.fine("Warmup: 50k iterations"); // TODO always use LOGGER.xxx( ()->yyyy )
        for (int i = 0; i < 50_000; i++) {
            buffer.clear();
            pickler.serialize(buffer, data);
        }
        LOGGER.info("Warmup complete"); // TODO always use LOGGER.xxx( ()->yyyy )
    }
    
    private static void profileWrites(Pickler<TestRecord> pickler, ByteBuffer buffer, TestRecord data) throws Exception {
        final long startTime = System.currentTimeMillis();
        final long endTime = startTime + 10_000; // 10 seconds
        int iterations = 0;
        
        // Hot loop - no logging here
        while (System.currentTimeMillis() < endTime) {
            buffer.clear();
            pickler.serialize(buffer, data);
            iterations++;
        }
        
        final long duration = System.currentTimeMillis() - startTime;
        final double opsPerSecond = (double) iterations / (duration / 1000.0);
        
        LOGGER.info(String.format("NFP Performance: %.0f ops/s (%d iterations in %d ms)", // TODO always use LOGGER.xxx( ()->yyyy ) 
                         opsPerSecond, iterations, duration));
    }
}
