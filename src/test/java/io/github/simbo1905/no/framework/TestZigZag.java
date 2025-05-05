package io.github.simbo1905.no.framework;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import java.nio.ByteBuffer;

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

    private ByteBuffer writeAndReset(long value) {
        ByteBuffer buffer = ByteBuffer.allocate(9);
        ZigZagEncoding.putLong(buffer, value);
        buffer.flip();
        return buffer;
    }

    private ByteBuffer writeAndReset(int value) {
        ByteBuffer buffer = ByteBuffer.allocate(5);
        ZigZagEncoding.putInt(buffer, value);
        buffer.flip();
        return buffer;
    }
}
