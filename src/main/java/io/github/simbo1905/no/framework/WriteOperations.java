package io.github.simbo1905.no.framework;

import java.nio.ByteBuffer;
import java.util.Map;

public class WriteOperations {
    
    /**
     * Writes a signed long value to the given buffer in LEB128 format
     * @param buffer The buffer to write to
     * @param value The long value to write
     * @return The number of bytes written
     */
    static int putLong(ByteBuffer buffer, long value) {
        if (buffer == null) {
            throw new IllegalArgumentException("Buffer cannot be null");
        }
        
        int count = 0;
        while ((value & 0xFFFFFFFFFFFFFF80L) != 0) {
            buffer.put((byte) (value & 0x7F));
            value >>= 7;
            count++;
        }
        buffer.put((byte) (value & 0x7F));
        count++;
        return count;
    }

    /**
     * Writes a long value to the given buffer in LEB128 ZigZag encoded format
     * @param buffer The buffer to write to
     * @param value The long value to write
     * @return The number of bytes written
     */
    static int putLongZigZag(ByteBuffer buffer, long value) {
        if (buffer == null) {
            throw new IllegalArgumentException("Buffer cannot be null");
        }
        
        int count = 0;
        // Apply ZigZag encoding
        value = (value << 1) ^ (value >> 63);
        while ((value & 0xFFFFFFFFFFFFFF80L) != 0) {
            buffer.put((byte) (value & 0x7F));
            value >>= 7;
            count++;
        }
        buffer.put((byte) (value & 0x7F));
        count++;
        return count;
    }

    static void write(Map<Class<?>, Integer> classToOffset, ByteBuffer buffer, Object c) {
        throw new AssertionError("not implemented");
    }

    static Object deserializeValue(Map<Integer, Class<?>> bufferOffset2Class, ByteBuffer buffer) {
        throw new AssertionError("Not implemented");
    }
}
