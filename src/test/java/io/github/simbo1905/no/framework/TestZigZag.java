package io.github.simbo1905.no.framework;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestZigZag {

    @Test
    void testMaxLongValue() {
        long maxValue = Long.MAX_VALUE; // 0x7FFFFFFFFFFFFFFF
        ByteBuffer buffer = ByteBuffer.allocate(9);
        
        ZigZagEncoding.putLong(buffer, maxValue);
        buffer.flip();
        
        long result = ZigZagEncoding.getLong(buffer);
        assertEquals(maxValue, result);
    }

    @Test
    void testZero() {
        long value = 0L;
        ByteBuffer buffer = ByteBuffer.allocate(1);
        
        ZigZagEncoding.putLong(buffer, value);
        buffer.flip();
        
        long result = ZigZagEncoding.getLong(buffer);
        assertEquals(value, result);
    }

    @Test
    void testPositiveNumbers() {
        long value = 1234567890L;
        ByteBuffer buffer = ByteBuffer.allocate(9);
        
        ZigZagEncoding.putLong(buffer, value);
        buffer.flip();
        
        long result = ZigZagEncoding.getLong(buffer);
        assertEquals(value, result);
    }

    @Test
    void testNegativeNumbers() {
        long value = -1234567890L;
        ByteBuffer buffer = ByteBuffer.allocate(9);
        
        ZigZagEncoding.putLong(buffer, value);
        buffer.flip();
        
        long result = ZigZagEncoding.getLong(buffer);
        assertEquals(value, result);
    }

    @Test
    void testMinLongValue() {
        long minValue = Long.MIN_VALUE; // 0x8000000000000000
        ByteBuffer buffer = ByteBuffer.allocate(9);
        
        ZigZagEncoding.putLong(buffer, minValue);
        buffer.flip();
        
        long result = ZigZagEncoding.getLong(buffer);
        assertEquals(minValue, result);
    }

    @Test
    void testIntValues() {
        int value = 12345;
        ByteBuffer buffer = ByteBuffer.allocate(5);
        
        ZigZagEncoding.putInt(buffer, value);
        buffer.flip();
        
        int result = ZigZagEncoding.getInt(buffer);
        assertEquals(value, result);
    }

    @Test
    void testNegativeIntValues() {
        int value = -12345;
        ByteBuffer buffer = ByteBuffer.allocate(5);
        
        ZigZagEncoding.putInt(buffer, value);
        buffer.flip();
        
        int result = ZigZagEncoding.getInt(buffer);
        assertEquals(value, result);
    }

  public static void main(String[] args) {
    ByteBuffer buffer = ByteBuffer.allocate(9);
    var results = new ArrayList<Record>();

    // Helper method to test a value
    testValue(buffer, 0L, true, results);
    testValue(buffer, 1L, true, results);

    // Test positive integers doubling up to Integer.MAX_VALUE
    long value = 1;
    while (value <= Integer.MAX_VALUE) {
      testValue(buffer, value, false, results);
      value *= 2;
    }

    // Test negative integers doubling down to Integer.MIN_VALUE
    value = -2;
    while (value >= Integer.MIN_VALUE) {
      testValue(buffer, value, false, results);
      value *= 2;
    }

    // Test positive longs doubling up to Long.MAX_VALUE
    value = 1;
    while (value > 0) {
      testValue(buffer, value, true, results);
      value *= 2;
    }

    // Print results in mod value order (0,1,-1,2,-2,4,-4...)
    results.sort(Comparator.comparingLong(v -> Math.abs((long) v.value)));
    System.out.println("Value\tBytes");
    System.out.println("-----\t-----");
    for (Record r : results) {
      System.out.println(r.value + "\t" + r.bytes);
    }
  }

  static void testValue(ByteBuffer buffer, long value, boolean isLong, List<Record> results) {
    buffer.clear();
    if (isLong) {
      ZigZagEncoding.putLong(buffer, value);
    } else {
      ZigZagEncoding.putInt(buffer, (int) value);
    }

    int bytesWritten = buffer.position();
    buffer.flip();

    long result = isLong ? ZigZagEncoding.getLong(buffer) : ZigZagEncoding.getInt(buffer);

    assert result == value;
    results.add(new Record(value, bytesWritten));
  }

  record Record(long value, int bytes) {
  }
}
