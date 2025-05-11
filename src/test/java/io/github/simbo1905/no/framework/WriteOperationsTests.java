package io.github.simbo1905.no.framework;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class WriteOperationsTests {
  @Test
  void testUnsignedEncoding() {
    ByteBuffer buf = ByteBuffer.allocate(10);
    int written = WriteOperations.writeVarInt(buf, 624485L);
    buf.flip();

    assertEquals(0x01, buf.get()); // Marker check
    byte[] expected = {(byte) 0xE5, (byte) 0x8E, 0x26};
    for (int i = 0; i < 3; i++) {
      assertEquals(expected[i], buf.get());
    }
    assertEquals(4, written); // 1 marker + 3 bytes
  }

  @Test
  void testSignedEncoding() {
    ByteBuffer buf = ByteBuffer.allocate(10);
    int written = WriteOperations.writeVarInt(buf, -12345L);
    buf.flip();

    assertEquals(0x02, buf.get()); // Marker check
    byte[] expected = {(byte) 0xC7, (byte) 0x9F, 0x7F};
    for (int i = 0; i < 3; i++) {
      assertEquals(expected[i], buf.get());
    }
    assertEquals(4, written); // 1 marker + 3 bytes
  }

  @Test
  void testZeroValue() {
    ByteBuffer buf = ByteBuffer.allocate(2);
    int written = WriteOperations.writeVarInt(buf, 0L);
    buf.flip();

    assertEquals(0x01, buf.get());
    assertEquals(0x00, buf.get());
    assertEquals(2, written);
  }

  @Test
  void testMax() {
    testRoundTrip(Long.MAX_VALUE);
  }

  @Test
  void testMin() {
    testRoundTrip(Long.MIN_VALUE);
  }

  private void testRoundTrip(long value) {
    ByteBuffer buf = ByteBuffer.allocate(12);
    WriteOperations.writeVarInt(buf, value);
    buf.flip();

    byte marker = buf.get();
    long decoded = WriteOperations.readVarInt(buf);

    assertEquals(value, decoded);
  }

}
