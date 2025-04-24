// SPDX-FileCopyrightText: 2025 Simon Massey
// SPDX-License-Identifier: Apache-2.0
package io.github.simbo1905.simple_pickle.tree;

/// Internal node that may have left and right children
public record InternalNode(String name, TreeNode left, TreeNode right) implements TreeNode {
    /**
     * Custom equals method that properly handles null children
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        
        InternalNode that = (InternalNode) o;
        
        if (!name.equals(that.name)) return false;
        if (left != null ? !left.equals(that.left) : that.left != null) return false;
        return right != null ? right.equals(that.right) : that.right == null;
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
