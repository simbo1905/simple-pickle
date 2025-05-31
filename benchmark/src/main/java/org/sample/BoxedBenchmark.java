package org.sample;

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import java.util.concurrent.TimeUnit;
import java.io.*;
import java.nio.ByteBuffer;
import io.github.simbo1905.no.framework.Pickler;

/// Phase 2: Benchmark for all boxed primitive types
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Thread)
@Warmup(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(1)
public class BoxedBenchmark {

    /// Test data: Record containing all boxed primitive types
    public record AllBoxed(
        Boolean boolVal,
        Byte byteVal, 
        Short shortVal,
        Character charVal,
        Integer intVal,
        Long longVal,
        Float floatVal,
        Double doubleVal
    ) implements Serializable {}

    // Test data - mix of null and non-null values
    static final AllBoxed testData = new AllBoxed(
        Boolean.TRUE, Byte.valueOf((byte)42), Short.valueOf((short)1000), 
        Character.valueOf('A'), Integer.valueOf(123456), Long.valueOf(9876543210L), 
        Float.valueOf(3.14f), Double.valueOf(2.71828)
    );

    static final AllBoxed testDataWithNulls = new AllBoxed(
        null, Byte.valueOf((byte)42), null, 
        Character.valueOf('A'), null, Long.valueOf(9876543210L), 
        null, Double.valueOf(2.71828)
    );

    // NFP
    private Pickler<AllBoxed> nfpPickler;

    @Setup(Level.Invocation)
    public void setup() {
        // Initialize NFP pickler
        nfpPickler = Pickler.forRecord(AllBoxed.class);
    }

    @Benchmark
    public void boxedNfp(Blackhole bh) throws Exception {
        // NFP serialization
        try (final var writeBuffer = nfpPickler.allocateForWriting(512)) { // Fair size based on measurements
            nfpPickler.serialize(writeBuffer, testData);
            final var readyToReadBack = writeBuffer.flip();
            
            // Deserialize
            final var readBuffer = nfpPickler.wrapForReading(readyToReadBack);
            AllBoxed result = nfpPickler.deserialize(readBuffer);
            bh.consume(result);
        }
    }

    @Benchmark  
    public void boxedJdk(Blackhole bh) throws Exception {
        // JDK serialization
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (ObjectOutputStream oos = new ObjectOutputStream(baos)) {
            oos.writeObject(testData);
        }
        
        // Deserialize
        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        try (ObjectInputStream ois = new ObjectInputStream(bais)) {
            AllBoxed result = (AllBoxed) ois.readObject();
            bh.consume(result);
        }
    }

    @Benchmark
    public void boxedWithNullsNfp(Blackhole bh) throws Exception {
        // NFP serialization with nulls
        try (final var writeBuffer = nfpPickler.allocateForWriting(512)) {
            nfpPickler.serialize(writeBuffer, testDataWithNulls);
            final var readyToReadBack = writeBuffer.flip();
            
            // Deserialize
            final var readBuffer = nfpPickler.wrapForReading(readyToReadBack);
            AllBoxed result = nfpPickler.deserialize(readBuffer);
            bh.consume(result);
        }
    }

    @Benchmark  
    public void boxedWithNullsJdk(Blackhole bh) throws Exception {
        // JDK serialization with nulls
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (ObjectOutputStream oos = new ObjectOutputStream(baos)) {
            oos.writeObject(testDataWithNulls);
        }
        
        // Deserialize
        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        try (ObjectInputStream ois = new ObjectInputStream(bais)) {
            AllBoxed result = (AllBoxed) ois.readObject();
            bh.consume(result);
        }
    }
}