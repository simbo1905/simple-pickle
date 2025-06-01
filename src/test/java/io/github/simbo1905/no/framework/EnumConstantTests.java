package io.github.simbo1905.no.framework;

import org.junit.jupiter.api.Test;

import java.util.HashSet;

import static org.junit.jupiter.api.Assertions.*;

/// Tests for enum constants in sealed interfaces - reproduces Paxos benchmark bug
class EnumConstantTests {

    /// Simple enum to test serialization
    enum Operation {
        NOOP, READ, WRITE, DELETE
    }

    /// Sealed interface with enum constant (reproduces bug pattern)
    sealed interface Command permits NoOperation, DataCommand {
        
        /// This constant causes serialization bug - NoOperation.NOOP fails with NPE
        NoOperation NOOP_INSTANCE = NoOperation.NOOP;
    }

    /// Record implementing sealed interface with enum constant
    record NoOperation(Operation op) implements Command {
        static final NoOperation NOOP = new NoOperation(Operation.NOOP);
    }

    /// Record with data implementing sealed interface  
    record DataCommand(byte[] data, Operation op) implements Command {}

    @Test
    void testEnumConstantSerialization() {
        // FIXME: This test will fail due to enum serialization bug
        // Error: Cannot invoke "String.getBytes(java.nio.charset.Charset)" because "internedName" is null
        // at io.github.simbo1905.no.framework.Writers.writeCompressedClassName

      final var allClasses = Companion.recordClassHierarchy(DataCommand.class, new HashSet<>()).toArray();

       final var pickler2 = Pickler.forRecord(DataCommand.class);

        final var pickler = Pickler.forSealedInterface(Command.class);
        
        // This should work but currently fails
        assertDoesNotThrow(() -> {
            final var buffer = pickler.allocateForWriting(1024);
            pickler.serialize(buffer, NoOperation.NOOP);
        }, "Enum constant serialization should not throw NPE");
    }

    @Test  
    void testEnumInRecordSerialization() {
        // FIXME: This test may also fail due to enum handling
        
        final var pickler = Pickler.forRecord(NoOperation.class);
        final var operation = new NoOperation(Operation.READ);
        
        assertDoesNotThrow(() -> {
            final var buffer = pickler.allocateForWriting(256);
            pickler.serialize(buffer, operation);
        }, "Enum in record serialization should work");
    }
}
