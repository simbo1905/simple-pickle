// SPDX-FileCopyrightText: 2025 Simon Massey
// SPDX-License-Identifier: Apache-2.0
package io.github.simbo1905.no.framework.tree;

/// A sealed interface representing a node in a tree structure
public sealed interface TreeNode permits InternalNode, LeafNode {
}
