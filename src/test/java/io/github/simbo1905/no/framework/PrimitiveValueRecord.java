package io.github.simbo1905.no.framework;

/**
 * Simple record class for testing primitive values
 */
public record PrimitiveValueRecord(
    boolean booleanValue,
    byte byteValue, 
    char charValue,
    short shortValue,
    int intValue,
    long longValue,
    float floatValue,
    double doubleValue
) {
}
