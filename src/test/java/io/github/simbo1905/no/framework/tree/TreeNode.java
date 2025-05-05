// SPDX-FileCopyrightText: 2025 Simon Massey
// SPDX-License-Identifier: Apache-2.0
package io.github.simbo1905.no.framework.tree;

/// A sealed interface representing a node in a tree structure
public sealed interface TreeNode permits InternalNode, LeafNode {
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
