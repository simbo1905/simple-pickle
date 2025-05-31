package com.github.trex_paxos;

import io.github.simbo1905.no.framework.Pickler;

import java.nio.ByteBuffer;
import java.util.UUID;

import static io.github.simbo1905.no.framework.Pickler.LOGGER;

public class Scaffolding {
  public record UserSession(String sessionId, UUID userId, long timestamp) {}

  public static void main(String[] args) throws Exception {
    final long mostSigBits = 0x550e8400e29b41d4L;
    final long leastSigBits = 0xa716446655440000L;
    final var originalUuid = new UUID(mostSigBits, leastSigBits);
    final var originalRecord = new UserSession("session123", originalUuid, System.currentTimeMillis());
    final var pickler = Pickler.forRecord(UserSession.class);
    final ByteBuffer readyToReadBack;
    
    // Write phase - serialize record
    try (final var writeBuffer = pickler.allocateForWriting(1024)) { //TODO: migrate all tests to use this pattern - using 1KB for fair comparison
      int actualSize = pickler.serialize(writeBuffer, originalRecord);
      LOGGER.info(() -> "Serialized record, actual size: " + actualSize + " bytes");
      
      readyToReadBack = writeBuffer.flip(); // flip() calls close(), buffer is now unusable
      LOGGER.info(() -> "Buffer ready for transmission, limit: " + readyToReadBack.limit());
      // In real usage: transmit readyToReadBack bytes to network or save to file
    }

    // Read phase - read back from transmitted/saved bytes
    final var readBuffer = pickler.wrapForReading(readyToReadBack);
    final var deserializedRecord = pickler.deserialize(readBuffer);
    LOGGER.info(() -> "Deserialized record: " + deserializedRecord);

    if( !originalRecord.equals(deserializedRecord)) {
      throw new AssertionError("Deserialized record does not match original");
    } else {
      LOGGER.info("Round-trip serialization successful: " + deserializedRecord);
    }
  }
}
