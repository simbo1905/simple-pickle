package com.github.trex_paxos;

import java.nio.ByteBuffer;
import java.util.UUID;
import io.github.simbo1905.no.framework.Pickler;

import static io.github.simbo1905.no.framework.Pickler.LOGGER;

public class Scaffolding {
  public record UserSession(String sessionId, UUID userId, long timestamp) {}

  public static void main(String[] args) throws Exception {
    final long mostSigBits = 0x550e8400e29b41d4L;
    final long leastSigBits = 0xa716446655440000L;
    final var originalUuid = new UUID(mostSigBits, leastSigBits);
    final var originalRecord = new UserSession("session123", originalUuid, System.currentTimeMillis());
    final var pickler = Pickler.forClass(UserSession.class);
    
    // Allocate buffer for serialization
    int maxSize = pickler.maxSizeOf(originalRecord);
    ByteBuffer buffer = ByteBuffer.allocate(maxSize);
    
    // Serialize record
    int actualSize = pickler.serialize(buffer, originalRecord);
    LOGGER.info(() -> "Serialized record, actual size: " + actualSize + " bytes");
    
    // Prepare for reading
    buffer.flip();
    LOGGER.info(() -> "Buffer ready for transmission, limit: " + buffer.limit());
    
    // Deserialize record
    final var deserializedRecord = pickler.deserialize(buffer);
    LOGGER.info(() -> "Deserialized record: " + deserializedRecord);

    if( !originalRecord.equals(deserializedRecord)) {
      throw new AssertionError("Deserialized record does not match original");
    } else {
      LOGGER.info("Round-trip serialization successful: " + deserializedRecord);
    }
  }
}
