package org.sample;

import java.io.Serializable;
import java.util.Objects;

public sealed interface TreeNode permits TreeNode.InternalNode, TreeNode.LeafNode {
  record LeafNode(int value) implements TreeNode, Serializable {
  }

  record InternalNode(String name, TreeNode left, TreeNode right) implements TreeNode, Serializable {
    /**
     * Custom equals method that properly handles null children
     */
    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      InternalNode that = (InternalNode) o;

      if (!name.equals(that.name)) return false;
      if (!Objects.equals(left, that.left)) return false;
      return Objects.equals(right, that.right);
    }

    /**
     * Custom hashCode method that properly handles null children
     */
    @Override
    public int hashCode() {
      int result = name.hashCode();
      result = 31 * result + (left != null ? left.hashCode() : 0);
      result = 31 * result + (right != null ? right.hashCode() : 0);
      return result;
    }
  }
}

