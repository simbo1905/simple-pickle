package org.sample;

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import java.util.concurrent.TimeUnit;
import java.io.*;
import java.nio.ByteBuffer;
import io.github.simbo1905.no.framework.Pickler;

/// Phase 1: Benchmark for all primitive types
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Thread)
@Warmup(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(1)
public class PrimitiveBenchmark {

    /// Test data: Record containing all primitive types
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

    // Test data
    static final AllPrimitives testData = new AllPrimitives(
        true, (byte)42, (short)1000, 'A', 123456, 9876543210L, 3.14f, 2.71828
    );

    // NFP
    private Pickler<AllPrimitives> nfpPickler;
    
    // Buffers sized based on measurements (will be updated after SizeCalculator run)
    private ByteBuffer nfpBuffer = ByteBuffer.allocate(256); // Fair size: will measure actual
    private ByteBuffer jdkBuffer = ByteBuffer.allocate(256); // Fair size: will measure actual

    @Setup(Level.Invocation)
    public void setup() {
        // Initialize NFP pickler
        nfpPickler = Pickler.forRecord(AllPrimitives.class);
        
        // Reset buffers
        nfpBuffer.clear();
        jdkBuffer.clear();
    }

    @Benchmark
    public void primitivesNfp(Blackhole bh) throws Exception {
        // NFP serialization
        try (final var writeBuffer = nfpPickler.allocateForWriting(256)) { // Fair size based on measurements
            nfpPickler.serialize(writeBuffer, testData);
            final var readyToReadBack = writeBuffer.flip();
            
            // Deserialize
            final var readBuffer = nfpPickler.wrapForReading(readyToReadBack);
            AllPrimitives result = nfpPickler.deserialize(readBuffer);
            bh.consume(result);
        }
    }

    @Benchmark  
    public void primitivesJdk(Blackhole bh) throws Exception {
        // JDK serialization
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (ObjectOutputStream oos = new ObjectOutputStream(baos)) {
            oos.writeObject(testData);
        }
        
        // Deserialize
        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        try (ObjectInputStream ois = new ObjectInputStream(bais)) {
            AllPrimitives result = (AllPrimitives) ois.readObject();
            bh.consume(result);
        }
    }
}