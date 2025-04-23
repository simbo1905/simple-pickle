// SPDX-FileCopyrightText: 2025 Simon Massey
// SPDX-License-Identifier: Apache-2.0
package io.github.simbo1905.simple_pickle.protocol;

public sealed interface StackCommand permits Push, Pop, Peek {
}
