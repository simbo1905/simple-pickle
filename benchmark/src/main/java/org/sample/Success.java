// SPDX-FileCopyrightText: 2025 Simon Massey
// SPDX-License-Identifier: Apache-2.0
package org.sample;

import java.util.Optional;

public record Success(Optional<String> value) implements StackResponse {
  public String payload() {
    return value.orElse(null);
  }
}
