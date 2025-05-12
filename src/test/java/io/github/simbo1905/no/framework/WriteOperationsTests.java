package io.github.simbo1905.no.framework;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.*;

public class WriteOperationsTests {
  @Test
  void testSmallInt() {
    ByteBuffer buf = ByteBuffer.allocate(4);
    WriteOperations ops = new WriteOperations(buf);
    int written = ops.write(100);
    buf.flip();

    assertEquals(100, ops.read());
    assertEquals(1 + Math.ceil((double) 100 / (double) (127 / 2)), written);
    assertFalse(buf.hasRemaining());

  }

  @Test
  void testSignedEncoding() {
    ByteBuffer buf = ByteBuffer.allocate(8);
    WriteOperations ops = new WriteOperations(buf);

    int written = ops.write(-12345L);
    buf.flip();

    assertEquals(-12345L, ops.read());
    assertEquals(4, written);
    assertFalse(buf.hasRemaining());
  }

  @Test
  void testZeroLong() {
    ByteBuffer buf = ByteBuffer.allocate(8);
    WriteOperations ops = new WriteOperations(buf);
    int written = ops.write(0L);
    buf.flip();

    assertEquals(0L, ops.read());
    assertEquals(2, written);
    assertFalse(buf.hasRemaining());
  }

  @Test
  void testZeroInt() {
    ByteBuffer buf = ByteBuffer.allocate(8);
    WriteOperations ops = new WriteOperations(buf);
    int written = ops.write(0);
    buf.flip();

    assertEquals(0x00, ops.read());
    assertEquals(1 + 1, written);
    assertFalse(buf.hasRemaining());
  }

  @Test
  void testMaxInt() {
    ByteBuffer buf = ByteBuffer.allocate(1 + Integer.BYTES);
    WriteOperations ops = new WriteOperations(buf);
    int written = ops.write(Integer.MAX_VALUE);
    buf.flip();

    assertEquals(Integer.MAX_VALUE, ops.read());
    assertEquals(1 + Integer.BYTES, written);
    assertFalse(buf.hasRemaining());
  }


  @Test
  void testMinInt() {
    ByteBuffer buf = ByteBuffer.allocate(1 + Integer.BYTES);
    WriteOperations ops = new WriteOperations(buf);
    int written = ops.write(Integer.MIN_VALUE);
    buf.flip();

    assertEquals(Integer.MIN_VALUE, ops.read());
    assertEquals(1 + Integer.BYTES, written);
    assertFalse(buf.hasRemaining());
  }

  @Test
  void testMaxLong() {
    ByteBuffer buf = ByteBuffer.allocate(1 + Long.BYTES);
    WriteOperations ops = new WriteOperations(buf);

    final var written = ops.write(Long.MAX_VALUE);
    buf.flip();

    final var decoded = ops.read();

    assertEquals(Long.MAX_VALUE, decoded);
    assertEquals(1 + Long.BYTES, written);
    assertFalse(buf.hasRemaining());
  }

  @Test
  void testMinLong() {
    ByteBuffer buf = ByteBuffer.allocate(1 + Long.BYTES);
    WriteOperations ops = new WriteOperations(buf);

    final var written = ops.write(Long.MIN_VALUE);
    buf.flip();

    final var decoded = ops.read();

    assertEquals(Long.MIN_VALUE, decoded);
    assertEquals(1 + Long.BYTES, written);
    assertFalse(buf.hasRemaining());
  }

  @Test
  void testBoolean() {
    final var buffer = ByteBuffer.allocate(4);
    WriteOperations ops = new WriteOperations(buffer);
    var written = ops.write(true);
    written += ops.write(false);
    buffer.flip();
    assertEquals(true, ops.read());
    assertEquals(false, ops.read());
    assertEquals(4, written);
    assertFalse(buffer.hasRemaining());
  }

  @Test
  void testFloat() {
    final var buffer = ByteBuffer.allocate(1 + Float.BYTES);
    WriteOperations ops = new WriteOperations(buffer);
    var written = ops.write(Float.MAX_VALUE);
    buffer.flip();
    assertEquals(Float.MAX_VALUE, ops.read());
    assertEquals(1 + Float.BYTES, written);
    assertFalse(buffer.hasRemaining());
  }

  @Test
  void testDouble() {
    final var buffer = ByteBuffer.allocate(1 + Double.BYTES);
    WriteOperations ops = new WriteOperations(buffer);
    var written = ops.write(Double.MAX_VALUE);
    buffer.flip();
    assertEquals(Double.MAX_VALUE, ops.read());
    assertEquals(1 + Double.BYTES, written);
    assertFalse(buffer.hasRemaining());
  }

  @Test
  void testShort() {
    final var buffer = ByteBuffer.allocate(1 + Short.BYTES);
    WriteOperations ops = new WriteOperations(buffer);
    var written = ops.write(Short.MAX_VALUE);
    buffer.flip();
    assertEquals(Short.MAX_VALUE, ops.read());
    assertEquals(1 + Short.BYTES, written);
    assertFalse(buffer.hasRemaining());
  }

  @Test
  void testChar() {
    final var buffer = ByteBuffer.allocate(1 + Character.BYTES);
    WriteOperations ops = new WriteOperations(buffer);
    var written = ops.write(Character.MAX_VALUE);
    buffer.flip();
    assertEquals(Character.MAX_VALUE, ops.read());
    assertEquals(1 + Character.BYTES, written);
    assertFalse(buffer.hasRemaining());
  }

  @Test
  void testString() {
    final var buffer = ByteBuffer.allocate(
        1 +
            ZigZagEncoding.sizeOf("hello world".length()) +
            "hello world".length()
    );
    WriteOperations ops = new WriteOperations(buffer);
    var written = ops.write("hello world");
    buffer.flip();
    assertEquals("hello world", ops.read());
    assertEquals(1 + "hello world".length(), written);
    assertFalse(buffer.hasRemaining());
  }

  @Test
  void testWriteNull() {
    final var buffer = ByteBuffer.allocate(
        1 + 1
    );
    WriteOperations ops = new WriteOperations(buffer);
    var written = ops.writeNull();
    buffer.flip();
    assertNull(ops.read());
    assertEquals(1, written);
    assertFalse(buffer.hasRemaining());
  }
}
