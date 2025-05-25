package com.github.trex_paxos;

import java.nio.ByteBuffer;
import java.util.UUID;
import io.github.simbo1905.no.framework.Pickler;

import static io.github.simbo1905.no.framework.Pickler.LOGGER;

public class Scaffolding {
  public record UserSession(String sessionId, UUID userId, long timestamp) {}

  public static void main(String[] args) {
    final long mostSigBits = 0x550e8400e29b41d4L;
    final long leastSigBits = 0xa716446655440000L;
    final var originalUuid = new UUID(mostSigBits, leastSigBits);
    final var originalRecord = new UserSession("session123", originalUuid, System.currentTimeMillis());
    final var pickler = Pickler.forRecord(UserSession.class);
    final int size = pickler.sizeOf(originalRecord);
    LOGGER.info(() -> "Calculated serialized size: " + size + " bytes");

    final var buffer = ByteBuffer.allocate(size);
    LOGGER.info(() -> "Allocated buffer with capacity: " + buffer.capacity());

    // Serialize the record
    pickler.serialize(originalRecord, buffer);
    LOGGER.info(() -> "Serialized record, buffer position: " + buffer.position());

    // Prepare buffer for reading
    buffer.flip();
    LOGGER.info(() -> "Buffer flipped for reading, limit: " + buffer.limit());

    // Deserialize the record
    final var deserializedRecord = pickler.deserialize(buffer);
    LOGGER.info(() -> "Deserialized record: " + deserializedRecord);

    if( !originalRecord.equals(deserializedRecord)) {
      throw new AssertionError("Deserialized record does not match original");
    } else {
      LOGGER.info("Round-trip serialization successful: " + deserializedRecord);
    }
  }
}
