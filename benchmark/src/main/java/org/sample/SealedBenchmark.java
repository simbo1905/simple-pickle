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
public class SealedBenchmark {

    // Simple sealed hierarchy
    public sealed interface Shape permits Circle, Rectangle, Triangle {}
    public record Circle(double radius) implements Shape, Serializable {}
    public record Rectangle(double width, double height) implements Shape, Serializable {}
    public record Triangle(double base, double height) implements Shape, Serializable {}
    
    public record ShapeContainer(Shape[] shapes) implements Serializable {}

    private Pickler<ShapeContainer> nfpPickler;
    private ShapeContainer testData;
    
    // For JDK serialization
    private ByteArrayOutputStream baos;

    @Setup
    public void setup() {
        testData = new ShapeContainer(new Shape[]{
            new Circle(5.0),
            new Rectangle(10.0, 20.0),
            new Triangle(15.0, 12.0),
            new Circle(3.0),
            new Rectangle(8.0, 6.0)
        });
        
        nfpPickler = Pickler.forClass(ShapeContainer.class);
        baos = new ByteArrayOutputStream(1024);
    }

    @Benchmark
    public ShapeContainer sealedNfp() {
        ByteBuffer buffer = ByteBuffer.allocate(512);
        nfpPickler.serialize(buffer, testData);
        buffer.flip();
        return nfpPickler.deserialize(buffer);
    }

    @Benchmark
    public ShapeContainer sealedJdk() throws Exception {
        baos.reset();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(testData);
        oos.close();
        
        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        ObjectInputStream ois = new ObjectInputStream(bais);
        return (ShapeContainer) ois.readObject();
    }
}