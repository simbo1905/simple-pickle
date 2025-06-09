package org.sample;

import io.github.simbo1905.no.framework.Pickler;
import org.openjdk.jmh.annotations.*;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(1)
public class MapBenchmark {

    public record Person(String name, int age) implements Serializable {}
    
    public record MapRecord(
        Map<String, Integer> stringIntMap,
        Map<String, Person> personMap,
        Map<Integer, Map<String, String>> nestedMap
    ) implements Serializable {}

    private Pickler<MapRecord> nfpPickler;
    private MapRecord testData;
    
    // For JDK serialization
    private ByteArrayOutputStream baos;

    @Setup
    public void setup() {
        testData = new MapRecord(
            Map.of("one", 1, "two", 2, "three", 3, "four", 4, "five", 5),
            Map.of(
                "alice", new Person("Alice", 30),
                "bob", new Person("Bob", 25),
                "charlie", new Person("Charlie", 35)
            ),
            Map.of(
                1, Map.of("a", "apple", "b", "banana"),
                2, Map.of("c", "cherry", "d", "date")
            )
        );
        
        nfpPickler = Pickler.forClass(MapRecord.class);
        baos = new ByteArrayOutputStream(1024);
    }

    @Benchmark
    public MapRecord mapNfp() {
        ByteBuffer buffer = ByteBuffer.allocate(512);
        nfpPickler.serialize(buffer, testData);
        buffer.flip();
        return nfpPickler.deserialize(buffer);
    }

    @Benchmark
    public MapRecord mapJdk() throws Exception {
        baos.reset();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(testData);
        oos.close();
        
        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        ObjectInputStream ois = new ObjectInputStream(bais);
        return (MapRecord) ois.readObject();
    }
}