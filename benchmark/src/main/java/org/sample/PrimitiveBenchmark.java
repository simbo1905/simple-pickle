package org.sample;

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import java.util.concurrent.TimeUnit;
import java.io.*;
import java.nio.ByteBuffer;
import io.github.simbo1905.no.framework.Pickler;
import io.github.simbo1905.no.framework.WriteBuffer;

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
    private WriteBuffer nfpWriteBuffer;
    
    // Pre-allocated streams for fair comparison with JDK
    private ByteArrayOutputStream baos;
    private ByteArrayInputStream bais;
    
    // Pre-serialized data for read-only benchmarks
    private ByteBuffer nfpSerializedData;
    private byte[] jdkSerializedData;

    @Setup(Level.Trial)
    public void setupTrial() throws Exception {
        // Initialize NFP pickler once
        nfpPickler = Pickler.forRecord(AllPrimitives.class);
        
        // Pre-serialize data for read-only benchmarks
        try (final var setupWriteBuffer = nfpPickler.allocateForWriting(256)) {
            nfpPickler.serialize(setupWriteBuffer, testData);
            nfpSerializedData = setupWriteBuffer.flip();
        }
        
        // Pre-serialize JDK data
        ByteArrayOutputStream setupBaos = new ByteArrayOutputStream();
        try (ObjectOutputStream oos = new ObjectOutputStream(setupBaos)) {
            oos.writeObject(testData);
        }
        jdkSerializedData = setupBaos.toByteArray();
    }

    @Setup(Level.Invocation)
    public void setup() {
        // Reset JDK streams for fair comparison
        baos = new ByteArrayOutputStream();
        // WriteBuffer cannot be reused after flip() - need fresh allocation per invocation
        nfpWriteBuffer = nfpPickler.allocateForWriting(256);
    }

    @Benchmark
    public void primitivesRoundtripNfp(Blackhole bh) throws Exception {
        // NFP round-trip serialization using pre-allocated WriteBuffer
        try (final var writeBuffer = nfpWriteBuffer) {
            nfpPickler.serialize(writeBuffer, testData);
            final var readyToReadBack = writeBuffer.flip();
            
            // Deserialize
            final var readBuffer = nfpPickler.wrapForReading(readyToReadBack);
            AllPrimitives result = nfpPickler.deserialize(readBuffer);
            bh.consume(result);
        }
    }

    @Benchmark  
    public void primitivesRoundtripJdk(Blackhole bh) throws Exception {
        // JDK round-trip serialization using pre-allocated stream
        try (ObjectOutputStream oos = new ObjectOutputStream(baos)) {
            oos.writeObject(testData);
        }
        
        // Deserialize
        bais = new ByteArrayInputStream(baos.toByteArray());
        try (ObjectInputStream ois = new ObjectInputStream(bais)) {
            AllPrimitives result = (AllPrimitives) ois.readObject();
            bh.consume(result);
        }
    }

    @Benchmark
    public void primitivesWriteNfp(Blackhole bh) throws Exception {
        // NFP write-only performance
        try (final var writeBuffer = nfpWriteBuffer) {
            nfpPickler.serialize(writeBuffer, testData);
            final var readyToReadBack = writeBuffer.flip();
            bh.consume(readyToReadBack);
        }
    }

    @Benchmark  
    public void primitivesWriteJdk(Blackhole bh) throws Exception {
        // JDK write-only performance
        try (ObjectOutputStream oos = new ObjectOutputStream(baos)) {
            oos.writeObject(testData);
            bh.consume(baos.toByteArray());
        }
    }

    @Benchmark
    public void primitivesReadNfp(Blackhole bh) throws Exception {
        // NFP read-only performance using pre-serialized data
        final var readBuffer = nfpPickler.wrapForReading(nfpSerializedData.duplicate());
        AllPrimitives result = nfpPickler.deserialize(readBuffer);
        bh.consume(result);
    }

    @Benchmark  
    public void primitivesReadJdk(Blackhole bh) throws Exception {
        // JDK read-only performance using pre-serialized data
        bais = new ByteArrayInputStream(jdkSerializedData);
        try (ObjectInputStream ois = new ObjectInputStream(bais)) {
            AllPrimitives result = (AllPrimitives) ois.readObject();
            bh.consume(result);
        }
    }
}