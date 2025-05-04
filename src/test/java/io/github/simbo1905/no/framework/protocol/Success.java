// SPDX-FileCopyrightText: 2025 Simon Massey
// SPDX-License-Identifier: Apache-2.0
package io.github.simbo1905.no.framework.protocol;

import java.util.Optional;

public record Success(Optional<String> value) implements StackResponse {
  public String payload() {
    return value.orElse(null);
  }
}
