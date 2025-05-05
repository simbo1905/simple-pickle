package org.sample;

import io.github.simbo1905.no.framework.Pickler;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import org.sample.proto.PushArray;
import org.sample.proto.PushMessage;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

@State(Scope.Thread)
@Warmup(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(1)
public class MyBenchmark {

  final ByteBuffer buffer = ByteBuffer.allocate(1024);
  final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream(1024);
  final ByteArrayOutputStream protobufOutputStream = new ByteArrayOutputStream(1024);

  @Setup(Level.Invocation)
  public void setupInvocation() {
    buffer.clear();
    byteArrayOutputStream.reset();
    protobufOutputStream.reset();
  }

  static final Push[] original = {
      new Push("hello"),
      new Push("world"),
      new Push("yes"),
      new Push("no"),
      new Push("hello"),
      new Push("world"),
      new Push("yes"),
      new Push("no"),
      new Push("hello"),
      new Push("world"),
      new Push("yes"),
      new Push("no"),
  };

  static int jdk = 0;
  static int nfp = 0;
  static int protobuf = 0;

  @TearDown(Level.Trial)
  public void tearDown() {
    // Teardown code that runs once after the entire benchmark
    System.out.println("Benchmark completed! nfp: " + nfp + ", jdk: " + jdk + ", protobuf: " + protobuf);
    // Print your results or summary here
  }

  public static void main(String[] args) {
    new MyBenchmark().testPickler1(null);
  }

  @Benchmark
  public void testPickler1(Blackhole bh) {

    Pickler.serializeMany(original, buffer);
    buffer.flip();
    nfp = buffer.remaining();
    final var back = Pickler.deserializeMany(Push.class, buffer);
    bh.consume(back);
  }

  @Benchmark
  public void testJdkSerialize1(Blackhole bh) throws IOException, ClassNotFoundException {
    // Clear the buffer before use
    buffer.clear();

    // Serialize to ByteBuffer via a channel
    try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
         ObjectOutputStream oos = new ObjectOutputStream(baos)) {
      oos.writeObject(original);
      byte[] bytes = baos.toByteArray();
      jdk = bytes.length;
      buffer.put(bytes);
    }

    // Flip the buffer for reading
    buffer.flip();

    // Deserialize from ByteBuffer
    byte[] readBytes = new byte[buffer.remaining()];
    buffer.get(readBytes);
    try (ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(readBytes))) {
      Push[] back = (Push[]) ois.readObject();
      bh.consume(back);
    }
  }

  @Benchmark
  public void testProtobuf1(Blackhole bh) throws IOException {
    // Clear the buffer before use
    buffer.clear();

    // Convert original Push objects to Protocol Buffer messages
    PushArray.Builder arrayBuilder = PushArray.newBuilder();
    for (Push push : original) {
      PushMessage pushMessage = PushMessage.newBuilder().setText(push.item()).build();
      arrayBuilder.addPushes(pushMessage);
    }
    PushArray pushArray = arrayBuilder.build();

    // Serialize to ByteBuffer
    byte[] serialized = pushArray.toByteArray();
    protobuf = serialized.length;
    buffer.put(serialized);

    // Flip the buffer for reading
    buffer.flip();

    // Deserialize from ByteBuffer
    byte[] readBytes = new byte[buffer.remaining()];
    buffer.get(readBytes);
    PushArray deserialized = PushArray.parseFrom(readBytes);

    // Convert back to Push objects
    Push[] result = new Push[deserialized.getPushesCount()];
    for (int i = 0; i < result.length; i++) {
      result[i] = new Push(deserialized.getPushes(i).getText());
    }

    bh.consume(result);
  }

}
