// SPDX-FileCopyrightText: 2025 Simon Massey
// SPDX-License-Identifier: MIT
package io.github.simbo1905.no.framework.animal;

public sealed interface Bird extends Animal permits Eagle, Penguin {
}
