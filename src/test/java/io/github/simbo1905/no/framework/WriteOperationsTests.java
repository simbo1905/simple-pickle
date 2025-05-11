package io.github.simbo1905.no.framework;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.ByteBuffer;
import java.util.logging.Logger;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class WriteOperationsTests {

    private static final Logger LOGGER = Logger.getLogger(WriteOperationsTests.class.getName());

    @Test
    void testPutLong() {
        // Test positive number
        ByteBuffer buffer = ByteBuffer.allocate(16);
        long value = 0x7FFFFFFFFFFFFFFFL; // Maximum positive long
        int expectedBytes = 8; // Long in LEB128 will take 8 bytes for max value
        
        int bytesWritten = WriteOperations.putLong(buffer, value);
        assertEquals(expectedBytes, bytesWritten, "Bytes written for max positive long");
        
        // Test negative number
        value = -1L;
        bytesWritten = WriteOperations.putLong(buffer, value);
        assertEquals(1, bytesWritten, "Bytes written for -1");
    }

    @Test
    void testPutLongZigZag() {
        // Test positive number with zigzag encoding
        ByteBuffer buffer = ByteBuffer.allocate(16);
        long value = 0x7FFFFFFFFFFFFFFFL; // Maximum positive long
        
        int bytesWritten = WriteOperations.putLongZigZag(buffer, value);
        assertEquals(8, bytesWritten, "Bytes written for max positive long with zigzag");
        
        // Test negative number with zigzag encoding
        value = -1L;
        bytesWritten = WriteOperations.putLongZigZag(buffer, value);
        assertEquals(1, bytesWritten, "Bytes written for -1 with zigzag");
    }

    @Test
    void testInvalidBuffer() {
        assertThrows(IllegalArgumentException.class, () -> {
            WriteOperations.putLong(null, 0L);
        }, "Null buffer should throw IllegalArgumentException");
    }
}
