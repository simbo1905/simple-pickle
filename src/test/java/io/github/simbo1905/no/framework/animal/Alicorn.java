// SPDX-FileCopyrightText: 2025 Simon Massey
// SPDX-License-Identifier: Apache-2.0
package io.github.simbo1905.no.framework.animal;

import java.util.Arrays;
import java.util.Objects;

public record Alicorn(String name, String[] magicPowers) implements Animal {
  @Override
  public boolean equals(Object o) {
    if (o == null || getClass() != o.getClass()) return false;
    Alicorn alicorn = (Alicorn) o;
    return Objects.equals(name, alicorn.name) && Objects.deepEquals(magicPowers, alicorn.magicPowers);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, Arrays.hashCode(magicPowers));
  }
}
