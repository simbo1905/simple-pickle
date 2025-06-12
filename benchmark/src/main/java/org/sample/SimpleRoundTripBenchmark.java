package org.sample;

import io.github.simbo1905.no.framework.Pickler;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import org.sample.proto.SimpleProto;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

/// Simple round-trip benchmark to validate NFP vs JDK full serialization cycle
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Thread)
@Warmup(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(1)
public class SimpleRoundTripBenchmark {

  /// Test record with multiple field types
  public record TestRecord(
      String name,
      int value,
      long timestamp,
      boolean active
  ) implements Serializable {}

  // Test data
  private static final TestRecord testData = new TestRecord("BenchmarkTest", 42, System.currentTimeMillis(), true);
  
  // NFP components
  private Pickler<TestRecord> nfpPickler;
  private ByteBuffer nfpBuffer;
  
  // JDK components  
  private ByteArrayOutputStream jdkOutputStream;

  @Setup(Level.Trial)
  public void setupTrial() throws Exception {
    // Initialize NFP pickler
    nfpPickler = Pickler.forClass(TestRecord.class);
    
    // Initialize JDK stream
    jdkOutputStream = new ByteArrayOutputStream();
    
    System.out.println("=== SimpleRoundTripBenchmark Test Data ===");
    System.out.println("Record: " + testData);
    System.out.println("NFP max size: " + nfpPickler.maxSizeOf(testData) + " bytes");
  }

  @Setup(Level.Invocation)
  public void setupInvocation() {
    // Reset NFP buffer for each invocation
    int maxSize = nfpPickler.maxSizeOf(testData);
    nfpBuffer = ByteBuffer.allocate(maxSize);
    
    // Reset JDK stream
    jdkOutputStream.reset();
  }

  @Benchmark
  public void nfp(Blackhole bh) throws Exception {
    nfpBuffer.clear();
    nfpPickler.serialize(nfpBuffer, testData);
    nfpBuffer.flip();
    TestRecord result = nfpPickler.deserialize(nfpBuffer);
    bh.consume(result);
  }
  
  @Benchmark
  public void jdk(Blackhole bh) throws Exception {
    jdkOutputStream.reset();
    try (ObjectOutputStream oos = new ObjectOutputStream(jdkOutputStream)) {
      oos.writeObject(testData);
    }
    
    try (ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(jdkOutputStream.toByteArray()))) {
      TestRecord result = (TestRecord) ois.readObject();
      bh.consume(result);
    }
  }
  
  @Benchmark
  public void protobuf(Blackhole bh) throws Exception {
    // Write phase
    SimpleProto.TestRecord protoRecord = SimpleProto.TestRecord.newBuilder()
        .setName(testData.name())
        .setValue(testData.value())
        .setTimestamp(testData.timestamp())
        .setActive(testData.active())
        .build();
    
    byte[] serialized = protoRecord.toByteArray();
    
    // Read phase
    SimpleProto.TestRecord parsed = SimpleProto.TestRecord.parseFrom(serialized);
    TestRecord result = new TestRecord(
        parsed.getName(),
        parsed.getValue(),
        parsed.getTimestamp(),
        parsed.getActive()
    );
    bh.consume(result);
  }
}