package io.github.simbo1905;

import io.github.simbo1905.no.framework.Pickler;
import io.github.simbo1905.no.framework.ReadBuffer;
import io.github.simbo1905.no.framework.tree.InternalNode;
import io.github.simbo1905.no.framework.tree.LeafNode;
import io.github.simbo1905.no.framework.tree.TreeNode;

public class TreeDemo {
  public static void main(String[] args) {

    final var leaf1 = new LeafNode(42);
    final var leaf2 = new LeafNode(99);
    final var leaf3 = new LeafNode(123);
// A lob sided tree
    final var internal1 = new InternalNode("Branch1", leaf1, leaf2);
    final var internal2 = new InternalNode("Branch2", leaf3, null);
    final var originalRoot = new InternalNode("root", internal1, internal2);

// Get a pickler for the TreeNode sealed interface
    final var pickler = Pickler.forSealedInterface(TreeNode.class);

// Allocate a buffer to hold just the root node
    final var buffer = pickler.allocateForWriting(pickler.maxSizeOf(originalRoot));

// Serialize only the root node (which should include the entire graph)
    pickler.serialize(buffer, originalRoot);

// Prepare buffer for reading
    final ReadBuffer buf = pickler.wrapForReading(buffer.flip());

// Deserialize the root node (which will reconstruct the entire graph)
    final var deserializedRoot = pickler.deserialize(buf);

// See junit tests that Validates the entire tree structure was properly deserialized
    TreeNode.areTreesEqual(originalRoot, deserializedRoot);
  }
}
