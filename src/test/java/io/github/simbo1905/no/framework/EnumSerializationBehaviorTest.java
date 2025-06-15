package io.github.simbo1905.no.framework;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeAll;
import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.*;

/// Test to demonstrate expected enum serialization behavior
/// These tests will fail initially (RED phase) then guide implementation
public class EnumSerializationBehaviorTest {

    @BeforeAll
    static void setupLogging() {
        io.github.simbo1905.LoggingControl.setupCleanLogging();
    }

    public enum Status {
        ACTIVE, INACTIVE, PENDING
    }
    
    public enum Priority {
        LOW, MEDIUM, HIGH
    }
    
    public record TaskWithEnum(String name, Status status) {}
    public record TaskWithEnumArray(String name, Status[] statuses) {}
    
    
    @Test
    void testEnumSignatureComputation() throws Exception {
        // Test that signature is computed from enum class + constant names
        
        // Get the actual signature that would be computed
        long statusSignature = PicklerImpl.hashEnumSignature(Status.class);
        long prioritySignature = PicklerImpl.hashEnumSignature(Priority.class);
        
        // Different enums should have different signatures
        assertNotEquals(statusSignature, prioritySignature, 
            "Different enums should have different signatures");
        
        System.out.println("Status signature: 0x" + Long.toHexString(statusSignature));
        System.out.println("Priority signature: 0x" + Long.toHexString(prioritySignature));
    }
    
    @Test 
    void testEnumInRecordUsesOrdinal() {
        TaskWithEnum task = new TaskWithEnum("My Task", Status.PENDING);
        Pickler<TaskWithEnum> pickler = Pickler.forClass(TaskWithEnum.class);
        
        ByteBuffer buffer = ByteBuffer.allocate(256);
        pickler.serialize(buffer, task);
        buffer.flip();
        
        int bytesWritten = buffer.limit();
        System.out.println("Record with enum serialized to " + bytesWritten + " bytes");
        
        // Expected format:
        // - Record type ordinal (1-2 bytes)
        // - Record signature (8 bytes)
        // - String "My Task": marker (1) + length (1) + bytes (7) = 9 bytes
        // - Enum: ENUM marker (1) + type ordinal (1) + signature (8) + constant ordinal (1) = 11 bytes
        // Total: ~31 bytes
        
        // Currently will be larger because enum is serialized as name string
        assertTrue(bytesWritten < 50, "Record with enum should be compact. Got: " + bytesWritten);
        
        TaskWithEnum deserialized = pickler.deserialize(buffer);
        assertEquals(task, deserialized);
    }
    
    @Test
    void testEnumWireFormatInRecord() {
        // More detailed test to verify exact wire format
        record SingleEnumRecord(Status status) {}
        
        SingleEnumRecord record = new SingleEnumRecord(Status.ACTIVE);
        Pickler<SingleEnumRecord> pickler = Pickler.forClass(SingleEnumRecord.class);
        
        ByteBuffer buffer = ByteBuffer.allocate(256);
        int startPos = buffer.position();
        pickler.serialize(buffer, record);
        int endPos = buffer.position();
        buffer.flip();
        
        int totalBytes = endPos - startPos;
        System.out.println("SingleEnumRecord serialized to " + totalBytes + " bytes");
        
        // Read back the wire format
        int recordTypeOrdinal = ZigZagEncoding.getInt(buffer);
        System.out.println("Record type ordinal: " + recordTypeOrdinal);
        
        long recordSignature = buffer.getLong();
        System.out.println("Record signature: 0x" + Long.toHexString(recordSignature));
        
        // Now we should see the enum
        int enumMarker = ZigZagEncoding.getInt(buffer);
        System.out.println("Enum marker: " + enumMarker + " (expected: " + Constants.ENUM.marker() + ")");
        assertEquals(Constants.ENUM.marker(), enumMarker, "Should see ENUM marker");
        
        int enumTypeOrdinal = ZigZagEncoding.getInt(buffer);
        System.out.println("Enum type ordinal: " + enumTypeOrdinal);
        
        long enumSignature = buffer.getLong();
        System.out.println("Enum signature: 0x" + Long.toHexString(enumSignature));
        
        // This is where we expect to see the ordinal but currently see name length
        int nextValue = ZigZagEncoding.getInt(buffer);
        System.out.println("Next value (should be ordinal 0 for ACTIVE): " + nextValue);
        
        // Currently this will FAIL because we write the name
        // After implementation, nextValue should be 0 (ordinal of ACTIVE)
        assertEquals(0, nextValue, "Should see enum ordinal 0 for ACTIVE, not name length");
    }
}