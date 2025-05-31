/// Quick debug utility to test NFP serialization sizes for benchmark data
/// Usage: java DebugSizes.java

import java.nio.ByteBuffer;
import java.util.logging.Logger;

/// Minimal test to debug buffer overflow issues
public class DebugSizes {
    static final Logger LOGGER = Logger.getLogger(DebugSizes.class.getName());
    
    public static void main(String[] args) {
        System.out.println("=== Debug NFP Serialization Sizes ===");
        
        // Simple test data
        record TestRecord(String name, int value) {}
        
        TestRecord simple = new TestRecord("test", 42);
        System.out.println("Test record: " + simple);
        
        // This will need to be expanded once we confirm basic patterns work
        System.out.println("Basic test completed - next step: add NFP dependency");
    }
}