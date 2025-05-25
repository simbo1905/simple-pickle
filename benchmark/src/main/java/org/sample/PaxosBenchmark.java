package org.sample;

import com.github.trex_paxos.BallotNumber;
import com.github.trex_paxos.Command;
import com.github.trex_paxos.NoOperation;
import com.github.trex_paxos.msg.Accept;
import io.github.simbo1905.no.framework.Pickler;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

@State(Scope.Thread)
@Warmup(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(1)
public class PaxosBenchmark {

  final ByteBuffer buffer = ByteBuffer.allocate(1024);
  final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream(1024);
  final ByteArrayOutputStream protobufOutputStream = new ByteArrayOutputStream(1024);

  @Setup(Level.Invocation)
  public void setupInvocation() {
    buffer.clear();
    byteArrayOutputStream.reset();
    protobufOutputStream.reset();
  }

  static final Accept[] original = {
      new Accept((short) 1, 2L, new BallotNumber((short) 3, 4, (short) 5), NoOperation.NOOP),
      new Accept((short) 6, 7L, new BallotNumber((short) 8, 9, (short) 10), new Command("data".getBytes(StandardCharsets.UTF_8), (byte)11)),
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
    new PaxosBenchmark().testPickler1(null);
  }

  @Benchmark
  public void testPickler1(Blackhole bh) {

    Pickler.serializeMany(original, buffer);
    buffer.flip();
    nfp = buffer.remaining();
    final var back = Pickler.deserializeMany(Accept.class, buffer);
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
      Accept[] back = (Accept[]) ois.readObject();
      bh.consume(back);
    }
  }
}
