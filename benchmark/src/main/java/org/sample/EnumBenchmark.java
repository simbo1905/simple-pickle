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
public class EnumBenchmark {

    public enum Status { PENDING, ACTIVE, COMPLETED, CANCELLED }
    public enum Priority { LOW, MEDIUM, HIGH, CRITICAL }
    
    public record EnumRecord(
        Status status,
        Priority priority,
        Status[] statusArray,
        Priority defaultPriority
    ) implements Serializable {}

    private Pickler<EnumRecord> nfpPickler;
    private EnumRecord testData;
    
    // For JDK serialization
    private ByteArrayOutputStream baos;

    @Setup
    public void setup() {
        testData = new EnumRecord(
            Status.ACTIVE,
            Priority.HIGH,
            new Status[]{Status.PENDING, Status.ACTIVE, Status.COMPLETED},
            Priority.MEDIUM
        );
        
        nfpPickler = Pickler.forClass(EnumRecord.class);
        baos = new ByteArrayOutputStream(1024);
    }

    @Benchmark
    public EnumRecord enumNfp() {
        ByteBuffer buffer = ByteBuffer.allocate(256);
        nfpPickler.serialize(buffer, testData);
        buffer.flip();
        return nfpPickler.deserialize(buffer);
    }

    @Benchmark
    public EnumRecord enumJdk() throws Exception {
        baos.reset();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(testData);
        oos.close();
        
        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        ObjectInputStream ois = new ObjectInputStream(bais);
        return (EnumRecord) ois.readObject();
    }
}