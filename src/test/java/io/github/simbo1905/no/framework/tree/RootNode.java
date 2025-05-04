// SPDX-FileCopyrightText: 2025 Simon Massey
// SPDX-License-Identifier: Apache-2.0
package io.github.simbo1905.no.framework.tree;

/// Root node of a tree with left and right children
public record RootNode(TreeNode left, TreeNode right) implements TreeNode {
}
