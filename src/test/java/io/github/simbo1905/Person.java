package io.github.simbo1905;

/// Define a simple record
/// The constructor must be public so that we can invoke the canonical constructor form the pickler package
public record Person(String name, int age) {
}
