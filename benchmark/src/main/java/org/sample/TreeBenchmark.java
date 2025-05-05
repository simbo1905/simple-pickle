package org.sample;

import io.github.simbo1905.no.framework.Pickler;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import org.sample.proto.*;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@State(Scope.Thread)
@Warmup(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(1)
public class TreeBenchmark {

  // The root node of our balanced tree
  private static TreeNode rootNode;

  // Buffer for Pickler and Protobuf
  private ByteBuffer buffer;

  // Buffer for JDK serialization
  private ByteArrayOutputStream byteArrayOutputStream;

  // Pickler for TreeNode sealed interface
  private static final Pickler<TreeNode> treeNodePickler = Pickler.forSealedInterface(TreeNode.class);

  @Setup(Level.Trial)
  public void setupTrial() {
    // Create a balanced tree with 100 leaf nodes
    rootNode = createBalancedTree(100);
  }

  @Setup(Level.Invocation)
  public void setupInvocation() {
    // Calculate buffer size for Pickler
    int bufferSize = treeNodePickler.sizeOf(rootNode);

    // Allocate a buffer that's large enough for all methods
    buffer = ByteBuffer.allocate(Math.max(bufferSize, 16384)); // 16KB should be enough
    buffer.clear();

    byteArrayOutputStream = new ByteArrayOutputStream(16384);
    byteArrayOutputStream.reset();
  }

  /**
   * Creates a balanced binary tree with the specified number of leaf nodes.
   *
   * @param leafCount The number of leaf nodes
   * @return The root node of the balanced tree
   */
  public static TreeNode createBalancedTree(int leafCount) {
    // Create an array of leaf nodes with values 0 to leafCount-1
    LeafNode[] leaves = new LeafNode[leafCount];
    for (int i = 0; i < leafCount; i++) {
      leaves[i] = new LeafNode(i);
    }

    // Build the tree bottom-up
    return buildBalancedTree(leaves, 0, leafCount - 1, 1);
  }

  static TreeNode buildBalancedTree(LeafNode[] leaves, int start, int end, int level) {
    if (start > end) {
      return null;
    }

    if (start == end) {
      return leaves[start];
    }

    int mid = (start + end) / 2;

    TreeNode left = buildBalancedTree(leaves, start, mid - 1, level + 1);
    TreeNode right = buildBalancedTree(leaves, mid + 1, end, level + 1);

    return new InternalNode("Node-" + level + "-" + mid, left, right);
  }

  public static void main(String[] args) {
    final var tb = new TreeBenchmark();
    tb.setupTrial();
    tb.setupInvocation();
    try {
      //tb.testJdkSerialize(null);
//      tb.testPicklerSerialize(null);
      tb.testProtobufSerialize(null);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Benchmark
  public void testJdkSerialize(Blackhole bh) throws IOException, ClassNotFoundException {
    // Clear the output stream
    byteArrayOutputStream.reset();

    // Serialize the tree using JDK serialization
    try (ObjectOutputStream oos = new ObjectOutputStream(byteArrayOutputStream)) {
      oos.writeObject(rootNode);
    }

    // write to buffer
    byte[] bytes = byteArrayOutputStream.toByteArray();
    buffer.put(bytes);

    buffer.flip();
    // Read from buffer
    byte[] readBytes = new byte[buffer.remaining()];
    buffer.get(readBytes);

    // Deserialize the tree
    try (ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(readBytes))) {
      TreeNode deserializedRoot = (TreeNode) ois.readObject();
      bh.consume(deserializedRoot);
    }
  }

  @Benchmark
  public void testPicklerSerialize(Blackhole bh) {
    // Clear the buffer
    buffer.clear();

    // Serialize the tree using Pickler
    treeNodePickler.serialize(rootNode, buffer);

    // Prepare buffer for reading
    buffer.flip();

    // Deserialize the tree
    TreeNode deserializedRoot = treeNodePickler.deserialize(buffer);
    bh.consume(deserializedRoot);
  }

  @Benchmark
  public void testProtobufSerialize(Blackhole bh) throws IOException {
    // Clear the buffer
    buffer.clear();

    // Convert to protobuf and serialize
    TreeNodeProto protoTree = convertToProto(rootNode);
    byte[] serialized = protoTree.toByteArray();
    // Write to buffer
    buffer.put(serialized);

    // Prepare buffer for reading
    buffer.flip();

    // Read from buffer
    byte[] bytes = new byte[buffer.remaining()];
    buffer.get(bytes);

    // Deserialize
    TreeNodeProto deserializedProto = TreeNodeProto.parseFrom(bytes);

    // Convert back to our model
    TreeNode deserializedRoot = convertFromProto(deserializedProto);

    bh.consume(deserializedRoot);
  }

  /**
   * Converts a TreeNode to its Protocol Buffer representation
   */
  private static TreeNodeProto convertToProto(TreeNode node) {
    if (node == null) {
      return null;
    }

    TreeNodeProto.Builder builder = TreeNodeProto.newBuilder();

    if (node instanceof LeafNode(int value)) {
      LeafNodeProto leafProto = LeafNodeProto.newBuilder()
          .setValue(value)
          .build();
      builder.setLeafNode(leafProto);
    } else if (node instanceof InternalNode(String name, TreeNode left, TreeNode right)) {
      InternalNodeProto.Builder internalBuilder = InternalNodeProto.newBuilder()
          .setName(name);

      if (left != null) {
        internalBuilder.setLeft(convertToProto(left));
      }

      if (right != null) {
        internalBuilder.setRight(convertToProto(right));
      }

      builder.setInternalNode(internalBuilder.build());
    }

    return builder.build();
  }

  /**
   * Converts a Protocol Buffer TreeNodeProto back to a TreeNode
   */
  private static TreeNode convertFromProto(TreeNodeProto proto) {
    if (proto == null) {
      return null;
    }

    switch (proto.getNodeTypeCase()) {
      case LEAF_NODE:
        LeafNodeProto leafProto = proto.getLeafNode();
        return new LeafNode(leafProto.getValue());

      case INTERNAL_NODE:
        InternalNodeProto internalProto = proto.getInternalNode();
        TreeNode left = null;
        TreeNode right = null;

        if (internalProto.hasLeft()) {
          left = convertFromProto(internalProto.getLeft());
        }

        if (internalProto.hasRight()) {
          right = convertFromProto(internalProto.getRight());
        }

        return new InternalNode(internalProto.getName(), left, right);

      default:
        throw new IllegalArgumentException("Unknown node type: " + proto.getNodeTypeCase());
    }
  }
}
