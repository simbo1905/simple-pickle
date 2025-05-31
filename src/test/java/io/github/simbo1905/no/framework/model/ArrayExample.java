// SPDX-FileCopyrightText: 2025 Simon Massey
// SPDX-License-Identifier: MIT
package io.github.simbo1905.no.framework.model;

import java.util.UUID;

public record ArrayExample(
    boolean[] booleanArray,
    byte[] byteArray,
    short[] shortArray,
    char[] charArray,
    int[] intArray,
    long[] longArray,
    float[] floatArray,
    double[] doubleArray,
    String[] stringArray,
    UUID[] uuidArray,
    Person[] personArray
) {
}
