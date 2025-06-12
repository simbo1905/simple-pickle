package org.sample;

import io.github.simbo1905.no.framework.Pickler;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import org.sample.proto.SimpleProto;
import org.json.JSONObject;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.TimeUnit;

/// Simple write-only benchmark to validate NFP vs JDK write performance
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Thread)
@Warmup(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(1)
public class SimpleWriteBenchmark {

  /// Test record with multiple field types
  public record TestRecord(
      String name,
      int value,
      long timestamp,
      boolean active
  ) implements Serializable {}

  // Test data
  private static final TestRecord testData = new TestRecord("BenchmarkTest", 42, System.currentTimeMillis(), true);

  @Setup(Level.Trial)
  public void measureSizes() throws Exception {
    JSONObject allSizes = new JSONObject();
    JSONObject testSizes = new JSONObject();
    
    // NFP size
    Pickler<TestRecord> nfpPickler = Pickler.forClass(TestRecord.class);
    ByteBuffer nfpBuffer = ByteBuffer.allocate(nfpPickler.maxSizeOf(testData));
    int nfpSize = nfpPickler.serialize(nfpBuffer, testData);
    testSizes.put(Source.NFP.name(), nfpSize);
    
    // JDK size
    ByteArrayOutputStream jdkStream = new ByteArrayOutputStream();
    try (ObjectOutputStream oos = new ObjectOutputStream(jdkStream)) {
      oos.writeObject(testData);
    }
    testSizes.put(Source.JDK.name(), jdkStream.size());
    
    // PTB size
    SimpleProto.TestRecord protoRecord = SimpleProto.TestRecord.newBuilder()
        .setName(testData.name())
        .setValue(testData.value())
        .setTimestamp(testData.timestamp())
        .setActive(testData.active())
        .build();
    testSizes.put(Source.PTB.name(), protoRecord.toByteArray().length);
    
    allSizes.put("SimpleWrite", testSizes);
    
    // Write to dated file
    String timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss"));
    Path sizesFile = Path.of("sizes-" + timestamp + ".json");
    Files.writeString(sizesFile, allSizes.toString(2));
    
    System.out.println("Sizes written to: " + sizesFile);
    System.out.println("SimpleWrite sizes: " + testSizes);
  }
  
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
    
    System.out.println("=== SimpleWriteBenchmark Test Data ===");
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
    int size = nfpPickler.serialize(nfpBuffer, testData);
    bh.consume(size);
  }

  @Benchmark
  public void jdk(Blackhole bh) throws Exception {
    jdkOutputStream.reset();
    try (ObjectOutputStream oos = new ObjectOutputStream(jdkOutputStream)) {
      oos.writeObject(testData);
    }
    bh.consume(jdkOutputStream.size());
  }

  @Benchmark
  public void protobuf(Blackhole bh) throws Exception {
    SimpleProto.TestRecord protoRecord = SimpleProto.TestRecord.newBuilder()
        .setName(testData.name())
        .setValue(testData.value())
        .setTimestamp(testData.timestamp())
        .setActive(testData.active())
        .build();
    
    byte[] serialized = protoRecord.toByteArray();
    bh.consume(serialized.length);
  }
}