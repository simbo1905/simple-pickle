package org.sample;

import io.github.simbo1905.no.framework.Pickler;
// Using direct ByteBuffer with new unified API
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import org.sample.proto.*;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

@State(Scope.Thread)
@Warmup(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(1)
public class TreeBenchmark {

  // The root node of our balanced tree
  private static TreeNode rootNode;

  // Buffer for Protobuf and JDK serialization
  private ByteBuffer buffer;

  // ByteBuffer for Pickler (unified API)
  private ByteBuffer picklerBuffer;

  // Buffer for JDK serialization
  private ByteArrayOutputStream byteArrayOutputStream;

  // Pickler for TreeNode sealed interface (unified API)
  private static final Pickler<TreeNode> treeNodePickler = Pickler.forClass(TreeNode.class);

  @Setup(Level.Trial)
  public void setupTrial() {
    // Create a balanced tree with 100 leaf nodes
    rootNode = createBalancedTree(100);
    
    // Pre-calculate and allocate NFP buffer once
    int bufferSize = treeNodePickler.maxSizeOf(rootNode);
    picklerBuffer = ByteBuffer.allocate(bufferSize);
    
    // Pre-allocate other buffers
    buffer = ByteBuffer.allocate(Math.max(bufferSize, 1024));
    byteArrayOutputStream = new ByteArrayOutputStream(Math.max(bufferSize, 1024));
  }

  @Setup(Level.Invocation)
  public void setupInvocation() {
    // Reset buffers for each invocation
    picklerBuffer.clear();
    buffer.clear();
    byteArrayOutputStream.reset();
  }

  @TearDown(Level.Trial)
  public void tearDown() {
    // Teardown code that runs once after the entire benchmark
    System.out.println("Benchmark completed!");
    // Print your results or summary here
  }

  /**
   * Creates a balanced binary tree with the specified number of leaf nodes.
   *
   * @param leafCount The number of leaf nodes
   * @return The root node of the balanced tree
   */
  public static TreeNode createBalancedTree(int leafCount) {
    // Create an array of leaf nodes with values 0 to leafCount-1
    TreeNode.LeafNode[] leaves = new TreeNode.LeafNode[leafCount];
    for (int i = 0; i < leafCount; i++) {
      leaves[i] = new TreeNode.LeafNode(i);
    }

    // Build the tree bottom-up
    return buildBalancedTree(leaves, 0, leafCount - 1, 1);
  }

  static TreeNode buildBalancedTree(TreeNode.LeafNode[] leaves, int start, int end, int level) {
    if (start > end) {
      return null;
    }

    if (start == end) {
      return leaves[start];
    }

    int mid = (start + end) / 2;

    TreeNode left = buildBalancedTree(leaves, start, mid - 1, level + 1);
    TreeNode right = buildBalancedTree(leaves, mid + 1, end, level + 1);

    return new TreeNode.InternalNode("Node-" + level + "-" + mid, left, right);
  }

  @Benchmark
  public void treeJdk(Blackhole bh) throws IOException, ClassNotFoundException {
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
  public void treeNfp(Blackhole bh) throws Exception {
    // Clear the buffer for writing
    picklerBuffer.clear();
    
    // Serialize tree using new unified API
    int actualSize = treeNodePickler.serialize(picklerBuffer, rootNode);
    
    // Prepare buffer for reading
    picklerBuffer.flip();
    
    // Deserialize tree
    TreeNode deserializedRoot = treeNodePickler.deserialize(picklerBuffer);
    bh.consume(deserializedRoot);
  }

  @Benchmark
  public void treeProtobuf(Blackhole bh) throws IOException {
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
  public static TreeNodeProto convertToProto(TreeNode node) {
    if (node == null) {
      return null;
    }

    TreeNodeProto.Builder builder = TreeNodeProto.newBuilder();

    if (node instanceof TreeNode.LeafNode(int value)) {
      LeafNodeProto leafProto = LeafNodeProto.newBuilder()
          .setValue(value)
          .build();
      builder.setLeafNode(leafProto);
    } else if (node instanceof TreeNode.InternalNode(String name, TreeNode left, TreeNode right)) {
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
        return new TreeNode.LeafNode(leafProto.getValue());

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

        return new TreeNode.InternalNode(internalProto.getName(), left, right);

      default:
        throw new IllegalArgumentException("Unknown node type: " + proto.getNodeTypeCase());
    }
  }
}
