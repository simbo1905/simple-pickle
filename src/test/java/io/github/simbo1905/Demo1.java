package io.github.simbo1905;


import io.github.simbo1905.no.framework.Pickler;

public class Demo1 {
  sealed interface TreeNode permits TreeNode.InternalNode, TreeNode.LeafNode {
    record LeafNode(int value) implements TreeNode {
    }

    record InternalNode(String name, TreeNode left, TreeNode right) implements TreeNode {
    }

    /// Sealed interfaces are exhaustively matched within matched pattern matching switch expressions
    static boolean areTreesEqual(TreeNode l, TreeNode r) {
      return switch (l) {
        case null -> r == null;
        case LeafNode(var v1) -> r instanceof LeafNode(var v2) && v1 == v2;
        case InternalNode(String n1, TreeNode i1, TreeNode i2) ->
            r instanceof InternalNode(String n2, TreeNode j1, TreeNode j2) &&
                n1.equals(n2) &&
                areTreesEqual(i1, j1) &&
                areTreesEqual(i2, j2);
      };
    }
  }

  public static void main(String[] args) {

    final var originalRoot = new TreeNode.InternalNode("Root",
        new TreeNode.InternalNode("Branch1", new TreeNode.LeafNode(42), new TreeNode.LeafNode(99)),
        new TreeNode.InternalNode("Branch2", new TreeNode.LeafNode(123), null));

// And a type safe pickler for the sealed interface:
    Pickler<TreeNode> treeNodePickler = Pickler.forSealedInterface(TreeNode.class);

// When we serialize a tree of nodes to a ByteBuffer:
    final var buffer = Pickler.allocate(1024);
    treeNodePickler.serialize(buffer, originalRoot);

// And deserialize it back:
    final var buf = buffer.flip();
    TreeNode deserializedRoot = treeNodePickler.deserialize(buf);

// Then it has elegantly and safely reconstructed the entire tree structure
    if (TreeNode.areTreesEqual(originalRoot, deserializedRoot)) {
      // This it true
    }
  }
}
