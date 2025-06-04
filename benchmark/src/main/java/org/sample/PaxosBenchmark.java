package org.sample;

import com.github.trex_paxos.BallotNumber;
import com.github.trex_paxos.Command;
import com.github.trex_paxos.NoOperation;
import com.github.trex_paxos.msg.Accept;
import io.github.simbo1905.no.framework.Pickler;
// Using direct ByteBuffer with new unified API
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import org.sample.proto.*;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

@State(Scope.Thread)
@Warmup(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(1)
public class PaxosBenchmark {

  final ByteBuffer buffer = ByteBuffer.allocate(1024);
  final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream(1024);
  final ByteArrayOutputStream protobufOutputStream = new ByteArrayOutputStream(1024);
  
  // Pre-allocated NFP components
  private Pickler<Accept> nfpPickler;
  private ByteBuffer nfpBuffer;

  @Setup(Level.Trial)
  public void setupTrial() throws Exception {
    // Initialize NFP pickler once
    nfpPickler = Pickler.of(Accept.class);
    
    // Calculate max size needed for all records
    int totalMaxSize = 0;
    for (var accept : original) {
      totalMaxSize += nfpPickler.maxSizeOf(accept);
    }
    nfpBuffer = ByteBuffer.allocate(totalMaxSize);
  }
  
  @Setup(Level.Invocation)
  public void setupInvocation() {
    buffer.clear();
    byteArrayOutputStream.reset();
    protobufOutputStream.reset();
    nfpBuffer.clear();
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

  @Benchmark
  public void paxosNfp(Blackhole bh) throws Exception {
    // Write phase - serialize all Accept records
    nfpBuffer.clear();
    for (var accept : original) {
      nfpPickler.serialize(nfpBuffer, accept);
    }
    nfpBuffer.flip();
    nfp = nfpBuffer.remaining();
    
    // Read phase - deserialize back
    final var back = new ArrayList<Accept>();
    for (int i = 0; i < original.length; i++) {
      back.add(nfpPickler.deserialize(nfpBuffer));
    }
    bh.consume(back);
  }

  @Benchmark
  public void paxosJdk(Blackhole bh) throws IOException, ClassNotFoundException {
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
    final Accept[] back;
    try (ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(readBytes))) {
      back = (Accept[]) ois.readObject();
    }
    bh.consume(back);
  }

  @Benchmark
  public void paxosProtobuf(Blackhole bh) throws IOException {
    // Clear the buffer
    buffer.clear();

    // Convert to protobuf array wrapper
    AcceptArray.Builder arrayBuilder = AcceptArray.newBuilder();
    for (Accept accept : original) {
      arrayBuilder.addAccepts(convertToProto(accept));
    }
    AcceptArray protoArray = arrayBuilder.build();

    // Serialize to buffer
    byte[] serialized = protoArray.toByteArray();
    protobuf = serialized.length;
    buffer.put(serialized);

    // Prepare buffer for reading
    buffer.flip();

    // Read from buffer
    byte[] bytes = new byte[buffer.remaining()];
    buffer.get(bytes);

    // Deserialize
    AcceptArray deserializedProtoArray = AcceptArray.parseFrom(bytes);

    // Convert back to Accept array
    Accept[] deserializedAccepts = new Accept[deserializedProtoArray.getAcceptsCount()];
    for (int i = 0; i < deserializedProtoArray.getAcceptsCount(); i++) {
      deserializedAccepts[i] = convertFromProto(deserializedProtoArray.getAccepts(i));
    }

    bh.consume(deserializedAccepts);
  }

  /**
   * Converts an Accept record to its Protocol Buffer representation
   */
  public static AcceptMessage convertToProto(Accept accept) {
    SlotTermMessage slotTermProto = SlotTermMessage.newBuilder()
        .setLogIndex(accept.slotTerm().logIndex())
        .setNumber(convertBallotNumberToProto(accept.slotTerm().number()))
        .build();

    AbstractCommandMessage commandProto = convertCommandToProto(accept.command());

    return AcceptMessage.newBuilder()
        .setFrom(accept.from())
        .setSlotTerm(slotTermProto)
        .setCommand(commandProto)
        .build();
  }

  /**
   * Converts a Protocol Buffer AcceptMessage back to an Accept record
   */
  private static Accept convertFromProto(AcceptMessage proto) {
    com.github.trex_paxos.SlotTerm slotTerm = new com.github.trex_paxos.SlotTerm(
        proto.getSlotTerm().getLogIndex(),
        convertBallotNumberFromProto(proto.getSlotTerm().getNumber())
    );

    com.github.trex_paxos.AbstractCommand command = convertCommandFromProto(proto.getCommand());

    return new Accept(
        (short) proto.getFrom(),
        slotTerm,
        command
    );
  }

  private static BallotNumberMessage convertBallotNumberToProto(com.github.trex_paxos.BallotNumber ballotNumber) {
    return BallotNumberMessage.newBuilder()
        .setEra(ballotNumber.era())
        .setCounter(ballotNumber.counter())
        .setNodeIdentifier(ballotNumber.nodeIdentifier())
        .build();
  }

  private static com.github.trex_paxos.BallotNumber convertBallotNumberFromProto(BallotNumberMessage proto) {
    return new com.github.trex_paxos.BallotNumber(
        (short) proto.getEra(),
        proto.getCounter(),
        (short) proto.getNodeIdentifier()
    );
  }

  private static AbstractCommandMessage convertCommandToProto(com.github.trex_paxos.AbstractCommand command) {
    AbstractCommandMessage.Builder builder = AbstractCommandMessage.newBuilder();

    if (command instanceof NoOperation) {
      builder.setNoOperation(NoOperationMessage.newBuilder().build());
    } else if (command instanceof Command cmd) {
      CommandMessage commandMsg = CommandMessage.newBuilder()
          .setUuid(cmd.uuid().toString())
          .setOperationBytes(com.google.protobuf.ByteString.copyFrom(cmd.operationBytes()))
          .setFlavour(cmd.flavour())
          .build();
      builder.setCommand(commandMsg);
    }

    return builder.build();
  }

  private static com.github.trex_paxos.AbstractCommand convertCommandFromProto(AbstractCommandMessage proto) {
    switch (proto.getCommandTypeCase()) {
      case NO_OPERATION:
        return NoOperation.NOOP;
      case COMMAND:
        CommandMessage cmdProto = proto.getCommand();
        return new Command(
            UUID.fromString(cmdProto.getUuid()),
            cmdProto.getOperationBytes().toByteArray(),
            (byte) cmdProto.getFlavour()
        );
      default:
        throw new IllegalArgumentException("Unknown command type: " + proto.getCommandTypeCase());
    }
  }

  public static void main(String[] args) throws Exception {
    System.out.println("=== Paxos Serialization Size Comparison ===");
    System.out.println("Test data: " + original.length + " Accept records");
    
    // Test JDK serialization
    ByteArrayOutputStream jdkOut = new ByteArrayOutputStream();
    try (ObjectOutputStream oos = new ObjectOutputStream(jdkOut)) {
      oos.writeObject(original);
    }
    int jdkSize = jdkOut.size();
    System.out.println("JDK Serialization size: " + jdkSize + " bytes");
    
    // Test No Framework Pickler
    final Pickler<Accept> pickler = Pickler.of(Accept.class);
    
    // Calculate total size needed
    int totalSize = 0;
    for (var accept : original) {
      totalSize += pickler.maxSizeOf(accept);
    }
    
    ByteBuffer writeBuffer = ByteBuffer.allocate(totalSize);
    for (var accept : original) {
      pickler.serialize(writeBuffer, accept);
    }
    writeBuffer.flip();
    int nfpSize = writeBuffer.remaining();
    System.out.println("No Framework Pickler size: " + nfpSize + " bytes");
    
    // Test Protobuf
    AcceptArray.Builder arrayBuilder = AcceptArray.newBuilder();
    for (Accept accept : original) {
      arrayBuilder.addAccepts(convertToProto(accept));
    }
    AcceptArray protoArray = arrayBuilder.build();
    byte[] protobufBytes = protoArray.toByteArray();
    int protobufSize = protobufBytes.length;
    System.out.println("Protobuf size: " + protobufSize + " bytes");
    
    // Calculate compression ratios
    System.out.println("\n=== Compression Ratios (vs JDK) ===");
    System.out.printf("NFP: %.2fx smaller\n", (double) jdkSize / nfpSize);
    System.out.printf("Protobuf: %.2fx smaller\n", (double) jdkSize / protobufSize);
  }
}
