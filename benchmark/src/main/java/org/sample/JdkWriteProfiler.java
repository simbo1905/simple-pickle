package org.sample;

import java.io.*;
import java.util.logging.Logger;

/// Isolated JDK write profiler with detailed logging
/// Run with: java -XX:StartFlightRecording=filename=jdk-write.jfr,duration=15s JdkWriteProfiler
public class JdkWriteProfiler {
    
    private static final Logger LOGGER = Logger.getLogger(JdkWriteProfiler.class.getName());

    public record TestRecord(String name, int value, long timestamp, boolean active) implements Serializable {}

    public static void main(String[] args) throws Exception {
        final var testData = new TestRecord("BenchmarkTest", 42, System.currentTimeMillis(), true);
        
        LOGGER.info("=== JDK Write Profiler ==="); // TODO always use LOGGER.xxx( ()->yyyy )
        LOGGER.info("Test data: " + testData); // TODO always use LOGGER.xxx( ()->yyyy )
        
        // Initialize JDK components
        LOGGER.fine("Creating ByteArrayOutputStream"); // TODO always use LOGGER.xxx( ()->yyyy )
        final var jdkOutputStream = new ByteArrayOutputStream();
        
        // Test serialization once to verify
        LOGGER.fine("Testing serialization"); // TODO always use LOGGER.xxx( ()->yyyy )
        jdkOutputStream.reset();
        try (ObjectOutputStream oos = new ObjectOutputStream(jdkOutputStream)) {
            oos.writeObject(testData);
        }
        final int serializedSize = jdkOutputStream.size();
        LOGGER.info("JDK serialized size: " + serializedSize + " bytes"); // TODO always use LOGGER.xxx( ()->yyyy )
        
        // Test deserialization
        final byte[] serializedData = jdkOutputStream.toByteArray();
        try (ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(serializedData))) {
            TestRecord deserialized = (TestRecord) ois.readObject();
            LOGGER.info("Deserialized: " + deserialized); // TODO always use LOGGER.xxx( ()->yyyy )
            LOGGER.info("Round-trip successful: " + testData.equals(deserialized)); // TODO always use LOGGER.xxx( ()->yyyy )
        }
        
        // Warm up
        LOGGER.info("Starting warmup"); // TODO always use LOGGER.xxx( ()->yyyy )
        warmUp(jdkOutputStream, testData);
        
        // Profile writes
        LOGGER.info("Starting write profiling (10 seconds)"); // TODO always use LOGGER.xxx( ()->yyyy )
        profileWrites(jdkOutputStream, testData);
        
        LOGGER.info("Profiling complete. Analyze with:"); // TODO always use LOGGER.xxx( ()->yyyy )
        LOGGER.info("jfr view hot-methods jdk-write.jfr"); // TODO always use LOGGER.xxx( ()->yyyy )
        LOGGER.info("jfr view allocation-by-site jdk-write.jfr"); // TODO always use LOGGER.xxx( ()->yyyy )
    }
    
    private static void warmUp(ByteArrayOutputStream stream, TestRecord data) throws Exception {
        LOGGER.fine("Warmup: 50k iterations"); // TODO always use LOGGER.xxx( ()->yyyy )
        for (int i = 0; i < 50_000; i++) {
            stream.reset();
            try (ObjectOutputStream oos = new ObjectOutputStream(stream)) {
                oos.writeObject(data);
            }
        }
        LOGGER.info("Warmup complete"); // TODO always use LOGGER.xxx( ()->yyyy )
    }
    
    private static void profileWrites(ByteArrayOutputStream stream, TestRecord data) throws Exception {
        final long startTime = System.currentTimeMillis();
        final long endTime = startTime + 10_000; // 10 seconds
        int iterations = 0;
        
        // Hot loop - no logging here
        while (System.currentTimeMillis() < endTime) {
            stream.reset();
            try (ObjectOutputStream oos = new ObjectOutputStream(stream)) {
                oos.writeObject(data);
            }
            iterations++;
        }
        
        final long duration = System.currentTimeMillis() - startTime;
        final double opsPerSecond = (double) iterations / (duration / 1000.0);
        
        LOGGER.info(String.format("JDK Performance: %.0f ops/s (%d iterations in %d ms)", // TODO always use LOGGER.xxx( ()->yyyy ) 
                         opsPerSecond, iterations, duration));
    }
}