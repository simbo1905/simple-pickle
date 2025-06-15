package io.github.simbo1905.no.framework;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeAll;
import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.*;

/// Test enum serialization to understand current behavior and fix inconsistency
public class EnumSerializationTest {

    @BeforeAll
    static void setupLogging() {
        io.github.simbo1905.LoggingControl.setupCleanLogging();
    }

    public enum Color {
        RED, GREEN, BLUE
    }
    
    public record SingleEnum(Color color) {}
    public record EnumArray(Color[] colors) {}
    
    @Test
    void testSingleEnumSerialization() {
        SingleEnum original = new SingleEnum(Color.GREEN);
        Pickler<SingleEnum> pickler = Pickler.forClass(SingleEnum.class);
        
        ByteBuffer buffer = ByteBuffer.allocate(256);
        int bytesWritten = pickler.serialize(buffer, original);
        buffer.flip();
        
        // Let's see what's written
        System.out.println("Single enum GREEN serialized in " + bytesWritten + " bytes");
        
        // Deserialize and verify
        SingleEnum deserialized = pickler.deserialize(buffer);
        assertEquals(Color.GREEN, deserialized.color());
    }
    
    @Test
    void testEnumArraySerialization() {
        Color[] colors = {Color.RED, Color.GREEN, Color.BLUE};
        EnumArray original = new EnumArray(colors);
        Pickler<EnumArray> pickler = Pickler.forClass(EnumArray.class);
        
        ByteBuffer buffer = ByteBuffer.allocate(256);
        int bytesWritten = pickler.serialize(buffer, original);
        buffer.flip();
        
        // Let's see what's written
        System.out.println("Enum array [RED, GREEN, BLUE] serialized in " + bytesWritten + " bytes");
        
        // Deserialize and verify
        EnumArray deserialized = pickler.deserialize(buffer);
        assertArrayEquals(colors, deserialized.colors());
    }
    
    @Test
    void testEnumSizeComparison() {
        // Compare sizes
        SingleEnum single = new SingleEnum(Color.GREEN);
        Pickler<SingleEnum> singlePickler = Pickler.forClass(SingleEnum.class);
        
        // Array with just one element for fair comparison
        EnumArray array = new EnumArray(new Color[]{Color.GREEN});
        Pickler<EnumArray> arrayPickler = Pickler.forClass(EnumArray.class);
        
        int singleSize = singlePickler.maxSizeOf(single);
        int arraySize = arrayPickler.maxSizeOf(array);
        
        System.out.println("Single enum maxSize: " + singleSize);
        System.out.println("Array[1] enum maxSize: " + arraySize);
        
        // The single enum should be smaller if we're writing ordinals
        // but currently it's bigger because we write the name
    }
}