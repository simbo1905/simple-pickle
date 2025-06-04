package org.sample;

import io.github.simbo1905.no.framework.Pickler;
import java.io.*;
import java.nio.ByteBuffer;
import java.util.logging.*;

public class DetailedLoggingProfiler {

    public record TestRecord(String name, int value, long timestamp, boolean active) implements Serializable {}

    public static void main(String[] args) throws Exception {
        // Set up FINEST logging for Pickler
        Logger picklerLogger = Logger.getLogger(Pickler.class.getName());
        picklerLogger.setLevel(Level.FINEST);
        for (Handler handler : Logger.getLogger("").getHandlers()) {
            handler.setLevel(Level.FINEST);
        }
        
        final var testData = new TestRecord("Test", 42, 123456789L, true);
        
        // Single run with logging
        final var pickler = Pickler.of(TestRecord.class);
        final var buffer = ByteBuffer.allocate(pickler.maxSizeOf(testData));
        
        long start = System.nanoTime();
        pickler.serialize(buffer, testData);
        long elapsed = System.nanoTime() - start;
        
        System.out.println("NFP: " + elapsed + " ns");
        
        // Compare with JDK
        var baos = new ByteArrayOutputStream();
        start = System.nanoTime();
        try (var oos = new ObjectOutputStream(baos)) {
            oos.writeObject(testData);
        }
        long jdkElapsed = System.nanoTime() - start;
        
        System.out.println("JDK: " + jdkElapsed + " ns");
        System.out.println("NFP/JDK ratio: " + ((double)elapsed/jdkElapsed));
    }
}