package org.sample;

import io.github.simbo1905.no.framework.Pickler;
import java.io.*;
import java.nio.ByteBuffer;

/// Single run profiler with FINEST logging to find performance issues
/// Run with: mvn exec:java -Dexec.mainClass="org.sample.SingleRunProfiler" -Djava.util.logging.ConsoleHandler.level=FINEST
public class SingleRunProfiler {

    public record TestRecord(String name, int value, long timestamp, boolean active) implements Serializable {}

    public static void main(String[] args) throws Exception {
        // Set up for detailed logging
        System.out.println("=== Single Run Profiler with FINEST Logging ===");
        System.out.println("Looking for performance issues in NFP vs JDK");
        System.out.println();
        
        final var testData = new TestRecord("SingleRunTest", 42, System.currentTimeMillis(), true);
        
        // NFP Single Run
        System.out.println("--- NFP Serialization (Single Run) ---");
        long nfpStart = System.nanoTime();
        
        final var nfpPickler = Pickler.of(TestRecord.class);
        final int maxSize = nfpPickler.maxSizeOf(testData);
        final var nfpBuffer = ByteBuffer.allocate(maxSize);
        
        // Single serialize operation
        int actualSize = nfpPickler.serialize(nfpBuffer, testData);
        
        long nfpEnd = System.nanoTime();
        long nfpTime = nfpEnd - nfpStart;
        
        System.out.println("NFP serialized size: " + actualSize + " bytes");
        System.out.println("NFP time: " + nfpTime + " ns (" + (nfpTime/1000) + " μs)");
        System.out.println();
        
        // JDK Single Run
        System.out.println("--- JDK Serialization (Single Run) ---");
        long jdkStart = System.nanoTime();
        
        final var jdkOutputStream = new ByteArrayOutputStream();
        try (ObjectOutputStream oos = new ObjectOutputStream(jdkOutputStream)) {
            oos.writeObject(testData);
        }
        
        long jdkEnd = System.nanoTime();
        long jdkTime = jdkEnd - jdkStart;
        
        System.out.println("JDK serialized size: " + jdkOutputStream.size() + " bytes");
        System.out.println("JDK time: " + jdkTime + " ns (" + (jdkTime/1000) + " μs)");
        System.out.println();
        
        // Comparison
        System.out.println("--- Comparison ---");
        System.out.println("NFP vs JDK time ratio: " + ((double)nfpTime/jdkTime) + "x");
        System.out.println("NFP vs JDK size ratio: " + ((double)actualSize/jdkOutputStream.size()) + "x");
        
        // Test deserialization to ensure correctness
        nfpBuffer.flip();
        TestRecord nfpDeserialized = nfpPickler.deserialize(nfpBuffer);
        System.out.println("NFP round-trip successful: " + testData.equals(nfpDeserialized));
    }
}