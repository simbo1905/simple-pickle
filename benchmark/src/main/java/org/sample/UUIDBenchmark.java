package org.sample;

import io.github.simbo1905.no.framework.Pickler;
import org.openjdk.jmh.annotations.*;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(1)
public class UUIDBenchmark {

    public record UUIDRecord(
        UUID id,
        UUID[] sessionIds,
        UUID nullable
    ) implements Serializable {}

    private Pickler<UUIDRecord> nfpPickler;
    private UUIDRecord testData;
    
    // For JDK serialization
    private ByteArrayOutputStream baos;

    @Setup
    public void setup() {
        testData = new UUIDRecord(
            UUID.randomUUID(),
            new UUID[]{
                UUID.randomUUID(),
                UUID.randomUUID(),
                UUID.randomUUID()
            },
            null
        );
        
        nfpPickler = Pickler.forClass(UUIDRecord.class);
        baos = new ByteArrayOutputStream(1024);
    }

    @Benchmark
    public UUIDRecord uuidNfp() {
        ByteBuffer buffer = ByteBuffer.allocate(256);
        nfpPickler.serialize(buffer, testData);
        buffer.flip();
        return nfpPickler.deserialize(buffer);
    }

    @Benchmark
    public UUIDRecord uuidJdk() throws Exception {
        baos.reset();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(testData);
        oos.close();
        
        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        ObjectInputStream ois = new ObjectInputStream(bais);
        return (UUIDRecord) ois.readObject();
    }
}