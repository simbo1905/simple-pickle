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
public class ArrayBenchmark {

    public record ArrayRecord(
        int[] intArray,
        byte[] byteArray,
        String[] stringArray
    ) implements Serializable {}

    private Pickler<ArrayRecord> nfpPickler;
    private ArrayRecord testData;
    
    // For JDK serialization
    private ByteArrayOutputStream baos;

    @Setup
    public void setup() {
        testData = new ArrayRecord(
            new int[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
            new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
            new String[]{"one", "two", "three", "four", "five"}
        );
        
        nfpPickler = Pickler.forClass(ArrayRecord.class);
        baos = new ByteArrayOutputStream(1024);
    }

    @Benchmark
    public ArrayRecord arrayNfp() {
        ByteBuffer buffer = ByteBuffer.allocate(512);
        nfpPickler.serialize(buffer, testData);
        buffer.flip();
        return nfpPickler.deserialize(buffer);
    }

    @Benchmark
    public ArrayRecord arrayJdk() throws Exception {
        baos.reset();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(testData);
        oos.close();
        
        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        ObjectInputStream ois = new ObjectInputStream(bais);
        return (ArrayRecord) ois.readObject();
    }
}