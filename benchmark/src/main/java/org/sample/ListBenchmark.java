package org.sample;

import io.github.simbo1905.no.framework.Pickler;
import org.openjdk.jmh.annotations.*;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(1)
public class ListBenchmark {

    // Note: List.of() returns immutable lists which ARE Serializable
    public record ListRecord(
        List<Integer> integerList,
        List<String> stringList,
        List<List<String>> nestedList
    ) implements Serializable {}

    private Pickler<ListRecord> nfpPickler;
    private ListRecord testData;
    
    // For JDK serialization
    private ByteArrayOutputStream baos;

    @Setup
    public void setup() {
        testData = new ListRecord(
            List.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10),
            List.of("one", "two", "three", "four", "five"),
            List.of(
                List.of("a", "b", "c"),
                List.of("d", "e", "f"),
                List.of("g", "h", "i")
            )
        );
        
        nfpPickler = Pickler.forClass(ListRecord.class);
        baos = new ByteArrayOutputStream(1024);
    }

    @Benchmark
    public ListRecord listNfp() {
        ByteBuffer buffer = ByteBuffer.allocate(512);
        nfpPickler.serialize(buffer, testData);
        buffer.flip();
        return nfpPickler.deserialize(buffer);
    }

    @Benchmark
    public ListRecord listJdk() throws Exception {
        baos.reset();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(testData);
        oos.close();
        
        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        ObjectInputStream ois = new ObjectInputStream(bais);
        return (ListRecord) ois.readObject();
    }
}