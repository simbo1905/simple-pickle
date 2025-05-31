// SPDX-FileCopyrightText: 2025 Simon Massey
// SPDX-License-Identifier: Apache-2.0
package org.sample;

public record Failure(String errorMessage) implements StackResponse {
  public String payload() {
    return errorMessage;
  }
}
