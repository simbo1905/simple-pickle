package org.sample;

import io.github.simbo1905.no.framework.Pickler;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

/// Simple standalone benchmark to validate NFP performance vs JDK
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Thread)
@Warmup(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(1)
public class SimpleBenchmark {

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
  private byte[] jdkSerializedData;

  @Setup(Level.Trial)
  public void setupTrial() throws Exception {
    // Initialize NFP pickler
    nfpPickler = Pickler.forClass(TestRecord.class);
    
    // Pre-serialize with JDK for read benchmarks
    jdkOutputStream = new ByteArrayOutputStream();
    try (ObjectOutputStream oos = new ObjectOutputStream(jdkOutputStream)) {
      oos.writeObject(testData);
    }
    jdkSerializedData = jdkOutputStream.toByteArray();
    
    System.out.println("=== SimpleBenchmark Test Data ===");
    System.out.println("Record: " + testData);
    System.out.println("NFP max size: " + nfpPickler.maxSizeOf(testData) + " bytes");
    System.out.println("JDK size: " + jdkSerializedData.length + " bytes");
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
  public void nfpWrite(Blackhole bh) throws Exception {
    nfpBuffer.clear();
    int size = nfpPickler.serialize(nfpBuffer, testData);
    bh.consume(size);
  }
  
  @Benchmark
  public void nfpRead(Blackhole bh) throws Exception {
    nfpBuffer.clear();
    nfpPickler.serialize(nfpBuffer, testData);
    nfpBuffer.flip();
    TestRecord result = nfpPickler.deserialize(nfpBuffer);
    bh.consume(result);
  }
  
  @Benchmark
  public void nfpRoundTrip(Blackhole bh) throws Exception {
    nfpBuffer.clear();
    nfpPickler.serialize(nfpBuffer, testData);
    nfpBuffer.flip();
    TestRecord result = nfpPickler.deserialize(nfpBuffer);
    bh.consume(result);
  }

  @Benchmark
  public void jdkWrite(Blackhole bh) throws Exception {
    jdkOutputStream.reset();
    try (ObjectOutputStream oos = new ObjectOutputStream(jdkOutputStream)) {
      oos.writeObject(testData);
    }
    bh.consume(jdkOutputStream.size());
  }
  
  @Benchmark
  public void jdkRead(Blackhole bh) throws Exception {
    try (ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(jdkSerializedData))) {
      TestRecord result = (TestRecord) ois.readObject();
      bh.consume(result);
    }
  }
  
  @Benchmark
  public void jdkRoundTrip(Blackhole bh) throws Exception {
    jdkOutputStream.reset();
    try (ObjectOutputStream oos = new ObjectOutputStream(jdkOutputStream)) {
      oos.writeObject(testData);
    }
    
    try (ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(jdkOutputStream.toByteArray()))) {
      TestRecord result = (TestRecord) ois.readObject();
      bh.consume(result);
    }
  }
}
