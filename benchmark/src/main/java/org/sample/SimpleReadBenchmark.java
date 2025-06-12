package org.sample;

import io.github.simbo1905.no.framework.Pickler;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import org.sample.proto.SimpleProto;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

/// Simple read-only benchmark to validate NFP vs JDK read performance
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Thread)
@Warmup(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(1)
public class SimpleReadBenchmark {

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
  private ByteBuffer nfpSerializedBuffer;
  
  // JDK components  
  private byte[] jdkSerializedData;
  
  // Protobuf components
  private byte[] protobufSerializedData;

  @Setup(Level.Trial)
  public void setupTrial() throws Exception {
    // Initialize NFP pickler
    nfpPickler = Pickler.forClass(TestRecord.class);
    
    // Pre-serialize with NFP for read benchmarks
    int maxSize = nfpPickler.maxSizeOf(testData);
    nfpSerializedBuffer = ByteBuffer.allocate(maxSize);
    nfpPickler.serialize(nfpSerializedBuffer, testData);
    nfpSerializedBuffer.flip();
    
    // Pre-serialize with JDK for read benchmarks
    ByteArrayOutputStream jdkOutputStream = new ByteArrayOutputStream();
    try (ObjectOutputStream oos = new ObjectOutputStream(jdkOutputStream)) {
      oos.writeObject(testData);
    }
    jdkSerializedData = jdkOutputStream.toByteArray();
    
    // Pre-serialize with Protobuf for read benchmarks
    SimpleProto.TestRecord protoRecord = SimpleProto.TestRecord.newBuilder()
        .setName(testData.name())
        .setValue(testData.value())
        .setTimestamp(testData.timestamp())
        .setActive(testData.active())
        .build();
    protobufSerializedData = protoRecord.toByteArray();
    
    System.out.println("=== SimpleReadBenchmark Test Data ===");
    System.out.println("Record: " + testData);
    System.out.println("NFP size: " + nfpSerializedBuffer.remaining() + " bytes");
    System.out.println("JDK size: " + jdkSerializedData.length + " bytes");
    System.out.println("PTB size: " + protobufSerializedData.length + " bytes");
  }

  @Benchmark
  public void nfp(Blackhole bh) throws Exception {
    nfpSerializedBuffer.rewind();
    TestRecord result = nfpPickler.deserialize(nfpSerializedBuffer);
    bh.consume(result);
  }
  
  @Benchmark
  public void jdk(Blackhole bh) throws Exception {
    try (ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(jdkSerializedData))) {
      TestRecord result = (TestRecord) ois.readObject();
      bh.consume(result);
    }
  }
  
  @Benchmark
  public void protobuf(Blackhole bh) throws Exception {
    SimpleProto.TestRecord protoRecord = SimpleProto.TestRecord.parseFrom(protobufSerializedData);
    TestRecord result = new TestRecord(
        protoRecord.getName(),
        protoRecord.getValue(),
        protoRecord.getTimestamp(),
        protoRecord.getActive()
    );
    bh.consume(result);
  }
}