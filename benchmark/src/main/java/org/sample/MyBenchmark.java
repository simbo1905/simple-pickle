package org.sample;

import io.github.simbo1905.no.framework.Pickler;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

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

  @Setup(Level.Invocation)
  public void setupInvocation() {
    buffer.clear();
    byteArrayOutputStream.reset();
  }

  @Benchmark
  public void testPickler1(Blackhole bh) {
    final Push[] original = {
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
    Pickler.serializeMany(original, buffer);
    buffer.flip();
    final var back = Pickler.deserializeMany(Push.class, buffer);
    bh.consume(back);
  }

  @Benchmark
  public void testJdkSerialize1(Blackhole bh) throws IOException, ClassNotFoundException {
    final Push[] original = {
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

    try (ObjectOutputStream oos = new ObjectOutputStream(byteArrayOutputStream)) {
      oos.writeObject(original);
    }

    try (ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(byteArrayOutputStream.toByteArray()))) {
      Push[] back = (Push[]) ois.readObject();
      bh.consume(back);
    }
  }
}
