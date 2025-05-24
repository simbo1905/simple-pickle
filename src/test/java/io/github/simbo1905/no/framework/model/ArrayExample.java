// SPDX-FileCopyrightText: 2025 Simon Massey
// SPDX-License-Identifier: MIT
package io.github.simbo1905.no.framework.model;

public record ArrayExample(
    int[] intArray,
    String[] stringArray,
    boolean[] booleanArray,
    Person[] personArray,
    Integer[] boxedIntArray,
    Object[] mixedArray
) {
}
