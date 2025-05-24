// SPDX-FileCopyrightText: 2025 Simon Massey
// SPDX-License-Identifier: MIT
package io.github.simbo1905.no.framework.protocol;

public sealed interface StackResponse permits Success, Failure {
  String payload();
}
