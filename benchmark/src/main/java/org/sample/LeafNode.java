// SPDX-FileCopyrightText: 2025 Simon Massey
// SPDX-License-Identifier: Apache-2.0
package org.sample;

import java.io.Serializable;

/// Leaf node with an integer value
public record LeafNode(int value) implements TreeNode, Serializable {
}
