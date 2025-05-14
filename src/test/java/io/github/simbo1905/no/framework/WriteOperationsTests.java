package io.github.simbo1905.no.framework;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

@SuppressWarnings("rawtypes")
public class WriteOperationsTests {
  @Test
  void testSmallInt() {
    ByteBuffer buf = ByteBuffer.allocate(4);
    CompactedBuffer ops = new CompactedBuffer(buf);
    int written = ops.write(100);
    buf.flip();

    assertEquals(100, ops.read());
    assertEquals(1 + Math.ceil((double) 100 / (double) (127 / 2)), written);
    assertFalse(buf.hasRemaining());

  }

  @Test
  void testSignedEncoding() {
    ByteBuffer buf = ByteBuffer.allocate(8);
    CompactedBuffer ops = new CompactedBuffer(buf);

    int written = ops.write(-12345L);
    buf.flip();

    assertEquals(-12345L, ops.read());
    assertEquals(4, written);
    assertFalse(buf.hasRemaining());
  }

  @Test
  void testZeroLong() {
    ByteBuffer buf = ByteBuffer.allocate(8);
    CompactedBuffer ops = new CompactedBuffer(buf);
    int written = ops.write(0L);
    buf.flip();

    assertEquals(0L, ops.read());
    assertEquals(2, written);
    assertFalse(buf.hasRemaining());
  }

  @Test
  void testZeroInt() {
    ByteBuffer buf = ByteBuffer.allocate(8);
    CompactedBuffer ops = new CompactedBuffer(buf);
    int written = ops.write(0);
    buf.flip();

    assertEquals(0x00, ops.read());
    assertEquals(1 + 1, written);
    assertFalse(buf.hasRemaining());
  }

  @Test
  void testMaxInt() {
    ByteBuffer buf = ByteBuffer.allocate(1 + Integer.BYTES);
    CompactedBuffer ops = new CompactedBuffer(buf);
    int written = ops.write(Integer.MAX_VALUE);
    buf.flip();

    assertEquals(Integer.MAX_VALUE, ops.read());
    assertEquals(1 + Integer.BYTES, written);
    assertFalse(buf.hasRemaining());
  }


  @Test
  void testMinInt() {
    ByteBuffer buf = ByteBuffer.allocate(1 + Integer.BYTES);
    CompactedBuffer ops = new CompactedBuffer(buf);
    int written = ops.write(Integer.MIN_VALUE);
    buf.flip();

    assertEquals(Integer.MIN_VALUE, ops.read());
    assertEquals(1 + Integer.BYTES, written);
    assertFalse(buf.hasRemaining());
  }

  @Test
  void testMaxLong() {
    ByteBuffer buf = ByteBuffer.allocate(1 + Long.BYTES);
    CompactedBuffer ops = new CompactedBuffer(buf);

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
    CompactedBuffer ops = new CompactedBuffer(buf);

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
    CompactedBuffer ops = new CompactedBuffer(buffer);
    var written = ops.write(true);
    written += ops.write(false);
    ops.flip();
    assertEquals(true, ops.read());
    assertEquals(false, ops.read());
    assertEquals(4, written);
    assertFalse(buffer.hasRemaining());
  }

  @Test
  void testFloat() {
    final var buffer = ByteBuffer.allocate(1 + Float.BYTES);
    CompactedBuffer ops = new CompactedBuffer(buffer);
    var written = ops.write(Float.MAX_VALUE);
    ops.flip();
    assertEquals(Float.MAX_VALUE, ops.read());
    assertEquals(1 + Float.BYTES, written);
    assertFalse(buffer.hasRemaining());
  }

  @Test
  void testDouble() {
    final var buffer = ByteBuffer.allocate(1 + Double.BYTES);
    CompactedBuffer ops = new CompactedBuffer(buffer);
    var written = ops.write(Double.MAX_VALUE);
    ops.flip();
    assertEquals(Double.MAX_VALUE, ops.read());
    assertEquals(1 + Double.BYTES, written);
    assertFalse(buffer.hasRemaining());
  }

  @Test
  void testShort() {
    final var buffer = ByteBuffer.allocate(1 + Short.BYTES);
    CompactedBuffer ops = new CompactedBuffer(buffer);
    var written = ops.write(Short.MAX_VALUE);
    ops.flip();
    assertEquals(Short.MAX_VALUE, ops.read());
    assertEquals(1 + Short.BYTES, written);
    assertFalse(buffer.hasRemaining());
  }

  @Test
  void testChar() {
    final var buffer = ByteBuffer.allocate(1 + Character.BYTES);
    CompactedBuffer ops = new CompactedBuffer(buffer);
    var written = ops.write(Character.MAX_VALUE);
    ops.flip();
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
    CompactedBuffer ops = new CompactedBuffer(buffer);
    var written = ops.write("hello world");
    ops.flip();
    assertEquals("hello world", ops.read());
    assertEquals(1 + "hello world".length(), written);
    assertFalse(buffer.hasRemaining());
  }

  @Test
  void testWriteNull() {
    final var buffer = ByteBuffer.allocate(
        1 + 1
    );
    CompactedBuffer ops = new CompactedBuffer(buffer);
    var written = ops.writeNull();
    ops.flip();
    assertNull(ops.read());
    assertEquals(1, written);
    assertFalse(buffer.hasRemaining());
  }

  @Test
  void testWriteOptionalEmpty() {
    final var buffer = ByteBuffer.allocate(
        1 + 1
    );
    final CompactedBuffer ops = new CompactedBuffer(buffer);
    final var written = ops.write(Optional.empty());
    ops.flip();
    final var read = ops.read();
    assertEquals(Optional.empty(), read);
    assertEquals(1, written);
    assertFalse(buffer.hasRemaining());
  }

  @Test
  void testWriteOptionalOfIntSmall() {
    final var buffer = ByteBuffer.allocate(
        3
    );
    final CompactedBuffer ops = new CompactedBuffer(buffer);
    final var written = ops.write(Optional.of(1));
    ops.flip();
    final var read = ops.read();
    assertEquals(Optional.of(1), read);
    assertEquals(3, written);
    assertFalse(buffer.hasRemaining());
  }

  @Test
  void testWriteOptionalOfIntLarge() {
    final var buffer = ByteBuffer.allocate(
        6
    );
    final CompactedBuffer ops = new CompactedBuffer(buffer);
    final var written = ops.write(Optional.of(Integer.MAX_VALUE));
    ops.flip();
    final var read = ops.read();
    assertEquals(Optional.of(Integer.MAX_VALUE), read);
    assertEquals(6, written);
    assertFalse(buffer.hasRemaining());
  }

  @Test
  void testWriteOptionalOfLongSmall() {
    final var buffer = ByteBuffer.allocate(
        3
    );
    final CompactedBuffer ops = new CompactedBuffer(buffer);
    final var written = ops.write(Optional.of(1L));
    ops.flip();
    final var read = ops.read();
    assertEquals(Optional.of(1L), read);
    assertEquals(3, written);
    assertFalse(buffer.hasRemaining());
  }

  @Test
  void testWriteOptionalOfLongLarge() {
    final var buffer = ByteBuffer.allocate(
        10
    );
    final CompactedBuffer ops = new CompactedBuffer(buffer);
    final var written = ops.write(Optional.of(Long.MAX_VALUE));
    ops.flip();
    final var read = ops.read();
    assertEquals(Optional.of(Long.MAX_VALUE), read);
    assertEquals(10, written);
    assertFalse(buffer.hasRemaining());
  }

  @Test
  void testWriteOptionalOfDouble() {
    final var buffer = ByteBuffer.allocate(
        10
    );
    final CompactedBuffer ops = new CompactedBuffer(buffer);
    final var written = ops.write(Optional.of(Double.MIN_VALUE));
    ops.flip();
    final var read = ops.read();
    assertEquals(Optional.of(Double.MIN_VALUE), read);
    assertEquals(10, written);
    assertFalse(buffer.hasRemaining());
  }

  @Test
  void testWriteOptionalOfFloat() {
    final var buffer = ByteBuffer.allocate(
        6
    );
    final CompactedBuffer ops = new CompactedBuffer(buffer);
    final var written = ops.write(Optional.of(Float.MIN_VALUE));
    ops.flip();
    final var read = ops.read();
    assertEquals(Optional.of(Float.MIN_VALUE), read);
    assertEquals(6, written);
    assertFalse(buffer.hasRemaining());
  }

  @Test
  void testWriteOptionalOfString() {
    final var buffer = ByteBuffer.allocate(
        2 + ZigZagEncoding.sizeOf("hello world".length()) + "hello world".length()
    );
    CompactedBuffer ops = new CompactedBuffer(buffer);
    var written = ops.write(Optional.of("hello world"));
    ops.flip();
    assertEquals(Optional.of("hello world"), ops.read());
    assertEquals(2 + "hello world".length(), written);
    assertFalse(buffer.hasRemaining());
  }

  @Test
  void testWriteType() {
    final var expectedSize =
        1 + ZigZagEncoding.sizeOf("Link".length()) + "Link".length();
    final var buffer = ByteBuffer.allocate(
        expectedSize
    );
    CompactedBuffer ops = new CompactedBuffer(buffer);
    final var type = new RecordPickler.InternedName("Link");
    var written = ops.write(type);
    ops.flip();
    assertEquals(type, ops.read());
    assertEquals(expectedSize, written);
    assertFalse(buffer.hasRemaining());
  }

  @Test
  void testEnum() {
    final var buffer = ByteBuffer.allocate(1024);
    CompactedBuffer ops = new CompactedBuffer(buffer);
    final var type = EnumTest.ONE;
    final var type2 = EnumTest.TWO;
    final var ignoredPrefix = "io.github.simbo1905.no.framework.";
    ops.write(ignoredPrefix, type);
    ops.write(ignoredPrefix, type2);
    ops.flip();
    assertEquals(new RecordPickler.InternedName("EnumTest.ONE"), ops.read());
    assertEquals(new RecordPickler.InternedName("EnumTest.TWO"), ops.read());
  }

  @Test
  void testEnumIntern() {
    final var buffer = ByteBuffer.allocate(1024);
    buffer.position(100);
    CompactedBuffer ops = new CompactedBuffer(buffer);
    final var one = EnumTest.ONE;
    final var ignoredPrefix = "io.github.simbo1905.no.framework.";
    ops.write(ignoredPrefix, one);
    ops.write(ignoredPrefix, one);
    ops.flip();
    buffer.position(100);
    assertEquals(new RecordPickler.InternedName("EnumTest.ONE"), ops.read());
    assertEquals(new RecordPickler.InternedName("EnumTest.ONE"), ops.read());
  }

  @Test
  void testNewWorld() {
    final var pickler = Pickler.forRecord(TestRecord.class);
    final var buffer = ByteBuffer.allocate(1024);
    final var serializationSession = new CompactedBuffer(buffer);

    final var testRecord = new TestRecord("Simbo", 42, EnumTest.ONE);
    pickler.serialize(serializationSession, testRecord);
    final var testRecord2 = new TestRecord("Fido", 3, EnumTest.TWO);
    pickler.serialize(serializationSession, testRecord2);


    final var readBuffer = serializationSession.flip();

    final var deserialized = pickler.deserialize(readBuffer);
    assertEquals(testRecord, deserialized);
    assertEquals(testRecord2, deserialized);
  }
}

record TestRecord(String name, int age, EnumTest testEnum) {
}

enum EnumTest {
  ONE("Link"),
  TWO("Link2");

  private final String lower;

  EnumTest(String lower) {
    this.lower = lower;
  }

  public String lower() {
    return lower;
  }
}
