package org.sample;

import io.github.simbo1905.no.framework.Pickler;
import org.openjdk.jmh.annotations.*;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(1)
public class OptionalBenchmark {

    // Skip JDK for Optional - it's not Serializable
    public record OptionalRecord(
        Optional<String> presentString,
        Optional<Integer> presentInteger,
        Optional<String> emptyString,
        Optional<Double> emptyDouble
    ) {}

    private Pickler<OptionalRecord> nfpPickler;
    private OptionalRecord testData;
    
    // For JDK serialization
    private ByteArrayOutputStream baos;

    @Setup
    public void setup() {
        testData = new OptionalRecord(
            Optional.of("Hello Optional"),
            Optional.of(42),
            Optional.empty(),
            Optional.empty()
        );
        
        nfpPickler = Pickler.forClass(OptionalRecord.class);
        baos = new ByteArrayOutputStream(1024);
    }

    @Benchmark
    public OptionalRecord optionalNfp() {
        ByteBuffer buffer = ByteBuffer.allocate(256);
        nfpPickler.serialize(buffer, testData);
        buffer.flip();
        return nfpPickler.deserialize(buffer);
    }

    // No JDK benchmark - Optional is not Serializable
}