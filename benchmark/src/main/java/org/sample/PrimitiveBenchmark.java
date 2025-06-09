package org.sample;

import io.github.simbo1905.no.framework.Pickler;
import org.openjdk.jmh.annotations.*;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(1)
public class PrimitiveBenchmark {

    public record PrimitiveRecord(
        boolean booleanValue,
        byte byteValue,
        short shortValue,
        char charValue,
        int intValue,
        long longValue,
        float floatValue,
        double doubleValue
    ) implements Serializable {}

    private Pickler<PrimitiveRecord> nfpPickler;
    private PrimitiveRecord testData;
    
    // For JDK serialization
    private ByteArrayOutputStream baos;
    private byte[] serializedJdk;

    @Setup
    public void setup() {
        testData = new PrimitiveRecord(
            true,
            (byte) 42,
            (short) 1337,
            'X',
            987654321,
            1234567890123456789L,
            3.14159f,
            2.718281828459045
        );
        
        nfpPickler = Pickler.forClass(PrimitiveRecord.class);
        baos = new ByteArrayOutputStream(1024);
        
        // Pre-serialize for JDK read benchmark
        try {
            baos.reset();
            ObjectOutputStream oos = new ObjectOutputStream(baos);
            oos.writeObject(testData);
            oos.close();
            serializedJdk = baos.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Benchmark
    public PrimitiveRecord primitiveNfp() {
        ByteBuffer buffer = ByteBuffer.allocate(256);
        nfpPickler.serialize(buffer, testData);
        buffer.flip();
        return nfpPickler.deserialize(buffer);
    }

    @Benchmark
    public PrimitiveRecord primitiveJdk() throws Exception {
        baos.reset();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(testData);
        oos.close();
        
        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        ObjectInputStream ois = new ObjectInputStream(bais);
        return (PrimitiveRecord) ois.readObject();
    }
}