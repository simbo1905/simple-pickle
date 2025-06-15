// SPDX-FileCopyrightText: 2025 Simon Massey
// SPDX-License-Identifier: MIT
package io.github.simbo1905.no.framework.model;

// Recursive containers are valid to any depth - this tests 2D and 3D arrays
public record NestedArrayExample(
    int[][] nestedIntArray,
    String[][] nestedStringArray,
    TestEnum[][][] nested3DEnumArray
) {
}

