package org.sample;

import io.github.simbo1905.no.framework.Pickler;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.TimeUnit;

@State(Scope.Thread)
@Warmup(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS) // 1 warmup iteration, 1 second long
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS) // 1 measurement iteration, 1 second long
@Fork(1) // Run in 1 fork
//@BenchmarkMode(Mode.AverageTime) // Optional: Specify mode if needed
//@OutputTimeUnit(TimeUnit.NANOSECONDS) // Optional: Specify time unit for results
public class MyBenchmark {

  final ByteBuffer buffer = ByteBuffer.allocate(1024);

  // This method runs BEFORE EACH call to helloWorld()
  @Setup(Level.Invocation)
  public void setupInvocation() {
    buffer.clear();
  }

  @Benchmark
  public void helloWorld(Blackhole bh) {
    final List<Push> original = List.of(
        new Push("hello"),
        new Push("world"),
        new Push("yes"),
        new Push("no"));
    int size = Pickler.sizeOfList(Push.class, original);
    ByteBuffer buffer = ByteBuffer.allocate(size);
    Pickler.serializeList(Push.class, original, buffer);
    buffer.flip();
    final var back = Pickler.deserializeList(Push.class, buffer);
    bh.consume(back);
  }
}
