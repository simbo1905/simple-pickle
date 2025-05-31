// SPDX-FileCopyrightText: 2025 Simon Massey
// SPDX-License-Identifier: MIT
package io.github.simbo1905.no.framework.protocol;

public record Failure(String errorMessage) implements StackResponse {
  public String payload() {
    return errorMessage;
  }
}
