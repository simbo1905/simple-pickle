// Test Java 21 single-file execution feature
// Run with: java TestJava21Feature.java

public class TestJava21Feature {
    public static void main(String[] args) {
        System.out.println("Java 21 single-file execution works!");
        System.out.println("No compilation step needed");
        
        // Test with a record
        record SimpleRecord(String name, int value) {}
        var record = new SimpleRecord("test", 42);
        System.out.println("Record: " + record);
    }
}