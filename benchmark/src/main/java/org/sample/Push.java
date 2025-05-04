// SPDX-FileCopyrightText: 2025 Simon Massey
// SPDX-License-Identifier: Apache-2.0
package org.sample;

public record Push(String item) implements StackCommand, java.io.Serializable {
}
