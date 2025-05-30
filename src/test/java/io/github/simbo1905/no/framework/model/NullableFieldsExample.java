// SPDX-FileCopyrightText: 2025 Simon Massey
// SPDX-License-Identifier: MIT
package io.github.simbo1905.no.framework.model;

import io.github.simbo1905.RefactorTests;

public record NullableFieldsExample(String stringField, Integer integerField, Double doubleField, RefactorTests.Animal objectField) {
}
