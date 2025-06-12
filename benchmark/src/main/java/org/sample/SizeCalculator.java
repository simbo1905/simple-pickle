package org.sample;

import com.github.trex_paxos.BallotNumber;
import com.github.trex_paxos.Command;
import com.github.trex_paxos.NoOperation;
import com.github.trex_paxos.msg.Accept;
import io.github.simbo1905.no.framework.Pickler;
import org.sample.proto.*;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * Calculates serialization sizes for benchmark data
 */
public class SizeCalculator {
    public static void main(String[] args) {
        try {
            // Test data - same as used in PaxosBenchmark
            Accept testAccept = new Accept((short) 1, 2L, new BallotNumber((short) 3, 4, (short) 5), NoOperation.NOOP);
            Accept testAcceptWithCommand = new Accept((short) 6, 7L, new BallotNumber((short) 8, 9, (short) 10), 
                new Command("data".getBytes(StandardCharsets.UTF_8), (byte)11));
            
            // Calculate NFP size
            int nfpSize = calculateNfpSize(testAccept, testAcceptWithCommand);
            
            // Calculate JDK size  
            int jdkSize = calculateJdkSize(testAccept, testAcceptWithCommand);
            
            // Calculate Protobuf size
            int ptbSize = calculateProtobufSize(testAccept, testAcceptWithCommand);
            
            // Calculate sizes for all benchmark types
            calculateAllSizes();
            
            // Output in parseable format (for legacy compatibility)
            System.out.println("NFP:" + nfpSize + ",JDK:" + jdkSize + ",PTB:" + ptbSize);
            
        } catch (Exception e) {
            System.err.println("Size calculation failed: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }
    
    private static int calculateNfpSize(Accept... accepts) throws Exception {
        Pickler<Accept> pickler = Pickler.forClass(Accept.class);
        int totalSize = 0;
        
        for (Accept accept : accepts) {
            ByteBuffer buffer = ByteBuffer.allocate(pickler.maxSizeOf(accept));
            pickler.serialize(buffer, accept);
            totalSize += buffer.position();
        }
        
        return totalSize;
    }
    
    private static int calculateJdkSize(Accept... accepts) throws Exception {
        int totalSize = 0;
        
        for (Accept accept : accepts) {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            try (ObjectOutputStream oos = new ObjectOutputStream(baos)) {
                oos.writeObject(accept);
            }
            totalSize += baos.size();
        }
        
        return totalSize;
    }
    
    private static int calculateProtobufSize(Accept... accepts) throws Exception {
        int totalSize = 0;
        
        // Use the same conversion logic as PaxosBenchmark
        AcceptArray.Builder arrayBuilder = AcceptArray.newBuilder();
        for (Accept accept : accepts) {
            arrayBuilder.addAccepts(PaxosBenchmark.convertToProto(accept));
        }
        AcceptArray protoArray = arrayBuilder.build();
        
        return protoArray.getSerializedSize();
    }
    
    private static void calculateAllSizes() throws Exception {
        Map<String, Map<String, Object>> allSizes = new LinkedHashMap<>();
        
        // SimpleWriteBenchmark  
        SimpleWriteBenchmark.TestRecord simpleRecord = new SimpleWriteBenchmark.TestRecord("Test Name", 42, 123456789L, true);
        String simpleTestData = "SimpleWriteBenchmark.TestRecord simpleRecord = new SimpleWriteBenchmark.TestRecord(\"Test Name\", 42, 123456789L, true);";
        allSizes.put("SimpleWrite", Map.of(
            "NFP", calculateSize(simpleRecord, Pickler.forClass(SimpleWriteBenchmark.TestRecord.class)),
            "JDK", calculateJdkSize(simpleRecord),
            "dataType", "SimpleWriteBenchmark.TestRecord",
            "testData", simpleTestData
        ));
        
        // PrimitiveBenchmark
        PrimitiveBenchmark.PrimitiveRecord primitiveRecord = new PrimitiveBenchmark.PrimitiveRecord(
            true, (byte)42, (short)1337, 'X', 987654321, 1234567890123456789L, 3.14159f, 2.718281828459045
        );
        String primitiveTestData = "PrimitiveBenchmark.PrimitiveRecord primitiveRecord = new PrimitiveBenchmark.PrimitiveRecord(\n    true, (byte)42, (short)1337, 'X', 987654321, 1234567890123456789L, 3.14159f, 2.718281828459045\n);";
        allSizes.put("PrimitiveBenchmark", Map.of(
            "NFP", calculateSize(primitiveRecord, Pickler.forClass(PrimitiveBenchmark.PrimitiveRecord.class)),
            "JDK", calculateJdkSize(primitiveRecord),
            "dataType", "PrimitiveBenchmark.PrimitiveRecord",
            "testData", primitiveTestData
        ));
        
        // StringBenchmark
        StringBenchmark.StringRecord stringRecord = new StringBenchmark.StringRecord(
            "Hello",
            "The quick brown fox jumps over the lazy dog",
            "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua."
        );
        String stringTestData = "StringBenchmark.StringRecord stringRecord = new StringBenchmark.StringRecord(\n    \"Hello\",\n    \"The quick brown fox jumps over the lazy dog\",\n    \"Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua.\"\n);";
        allSizes.put("StringBenchmark", Map.of(
            "NFP", calculateSize(stringRecord, Pickler.forClass(StringBenchmark.StringRecord.class)),
            "JDK", calculateJdkSize(stringRecord),
            "dataType", "StringBenchmark.StringRecord",
            "testData", stringTestData
        ));
        
        // ArrayBenchmark
        ArrayBenchmark.ArrayRecord arrayRecord = new ArrayBenchmark.ArrayRecord(
            new int[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
            new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
            new String[]{"one", "two", "three", "four", "five"}
        );
        String arrayTestData = "ArrayBenchmark.ArrayRecord arrayRecord = new ArrayBenchmark.ArrayRecord(\n    new int[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},\n    new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},\n    new String[]{\"one\", \"two\", \"three\", \"four\", \"five\"}\n);";
        allSizes.put("ArrayBenchmark", Map.of(
            "NFP", calculateSize(arrayRecord, Pickler.forClass(ArrayBenchmark.ArrayRecord.class)),
            "JDK", calculateJdkSize(arrayRecord),
            "dataType", "ArrayBenchmark.ArrayRecord",
            "testData", arrayTestData
        ));
        
        // ListBenchmark
        ListBenchmark.ListRecord listRecord = new ListBenchmark.ListRecord(
            List.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10),
            List.of("one", "two", "three", "four", "five"),
            List.of(List.of("a", "b", "c"), List.of("d", "e", "f"), List.of("g", "h", "i"))
        );
        String listTestData = "ListBenchmark.ListRecord listRecord = new ListBenchmark.ListRecord(\n    List.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10),\n    List.of(\"one\", \"two\", \"three\", \"four\", \"five\"),\n    List.of(List.of(\"a\", \"b\", \"c\"), List.of(\"d\", \"e\", \"f\"), List.of(\"g\", \"h\", \"i\"))\n);";
        allSizes.put("ListBenchmark", Map.of(
            "NFP", calculateSize(listRecord, Pickler.forClass(ListBenchmark.ListRecord.class)),
            "JDK", calculateJdkSize(listRecord),
            "dataType", "ListBenchmark.ListRecord",
            "testData", listTestData
        ));
        
        // OptionalBenchmark - NFP only (Optional not Serializable)
        OptionalBenchmark.OptionalRecord optionalRecord = new OptionalBenchmark.OptionalRecord(
            Optional.of("Hello Optional"), Optional.of(42), Optional.empty(), Optional.empty()
        );
        String optionalTestData = "OptionalBenchmark.OptionalRecord optionalRecord = new OptionalBenchmark.OptionalRecord(\n    Optional.of(\"Hello Optional\"), Optional.of(42), Optional.empty(), Optional.empty()\n);";
        allSizes.put("OptionalBenchmark", Map.of(
            "NFP", calculateSize(optionalRecord, Pickler.forClass(OptionalBenchmark.OptionalRecord.class)),
            "JDK", -1,  // Not applicable
            "dataType", "OptionalBenchmark.OptionalRecord",
            "testData", optionalTestData
        ));
        
        // MapBenchmark
        MapBenchmark.Person alice = new MapBenchmark.Person("Alice", 30);
        MapBenchmark.Person bob = new MapBenchmark.Person("Bob", 25);
        MapBenchmark.Person charlie = new MapBenchmark.Person("Charlie", 35);
        MapBenchmark.MapRecord mapRecord = new MapBenchmark.MapRecord(
            Map.of("one", 1, "two", 2, "three", 3, "four", 4, "five", 5),
            Map.of("alice", alice, "bob", bob, "charlie", charlie),
            Map.of(1, Map.of("a", "apple", "b", "banana"), 2, Map.of("c", "cherry", "d", "date"))
        );
        String mapTestData = "MapBenchmark.Person alice = new MapBenchmark.Person(\"Alice\", 30);\nMapBenchmark.Person bob = new MapBenchmark.Person(\"Bob\", 25);\nMapBenchmark.Person charlie = new MapBenchmark.Person(\"Charlie\", 35);\nMapBenchmark.MapRecord mapRecord = new MapBenchmark.MapRecord(\n    Map.of(\"one\", 1, \"two\", 2, \"three\", 3, \"four\", 4, \"five\", 5),\n    Map.of(\"alice\", alice, \"bob\", bob, \"charlie\", charlie),\n    Map.of(1, Map.of(\"a\", \"apple\", \"b\", \"banana\"), 2, Map.of(\"c\", \"cherry\", \"d\", \"date\"))\n);";
        allSizes.put("MapBenchmark", Map.of(
            "NFP", calculateSize(mapRecord, Pickler.forClass(MapBenchmark.MapRecord.class)),
            "JDK", calculateJdkSize(mapRecord),
            "dataType", "MapBenchmark.MapRecord",
            "testData", mapTestData
        ));
        
        // EnumBenchmark
        EnumBenchmark.EnumRecord enumRecord = new EnumBenchmark.EnumRecord(
            EnumBenchmark.Status.ACTIVE,
            EnumBenchmark.Priority.HIGH,
            new EnumBenchmark.Status[]{EnumBenchmark.Status.PENDING, EnumBenchmark.Status.ACTIVE, EnumBenchmark.Status.COMPLETED},
            EnumBenchmark.Priority.MEDIUM
        );
        String enumTestData = "EnumBenchmark.EnumRecord enumRecord = new EnumBenchmark.EnumRecord(\n    EnumBenchmark.Status.ACTIVE,\n    EnumBenchmark.Priority.HIGH,\n    new EnumBenchmark.Status[]{EnumBenchmark.Status.PENDING, EnumBenchmark.Status.ACTIVE, EnumBenchmark.Status.COMPLETED},\n    EnumBenchmark.Priority.MEDIUM\n);";
        allSizes.put("EnumBenchmark", Map.of(
            "NFP", calculateSize(enumRecord, Pickler.forClass(EnumBenchmark.EnumRecord.class)),
            "JDK", calculateJdkSize(enumRecord),
            "dataType", "EnumBenchmark.EnumRecord",
            "testData", enumTestData
        ));
        
        // UUIDBenchmark
        UUIDBenchmark.UUIDRecord uuidRecord = new UUIDBenchmark.UUIDRecord(
            UUID.randomUUID(),
            new UUID[]{UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID()},
            null
        );
        String uuidTestData = "UUIDBenchmark.UUIDRecord uuidRecord = new UUIDBenchmark.UUIDRecord(\n    UUID.randomUUID(),\n    new UUID[]{UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID()},\n    null\n);";
        allSizes.put("UUIDBenchmark", Map.of(
            "NFP", calculateSize(uuidRecord, Pickler.forClass(UUIDBenchmark.UUIDRecord.class)),
            "JDK", calculateJdkSize(uuidRecord),
            "dataType", "UUIDBenchmark.UUIDRecord",
            "testData", uuidTestData
        ));
        
        // Write sizes.json file
        try (java.io.FileWriter writer = new java.io.FileWriter("sizes.json")) {
            writer.write("{\n");
            boolean first = true;
            for (Map.Entry<String, Map<String, Object>> entry : allSizes.entrySet()) {
                String benchmark = entry.getKey();
                Map<String, Object> data = entry.getValue();
                Object jdkSize = data.get("JDK");
                if (jdkSize instanceof Integer && (Integer)jdkSize > 0) {
                    if (!first) writer.write(",\n");
                    writer.write("  \"" + benchmark + "\": {\n");
                    writer.write("    \"NFP\": " + data.get("NFP") + ",\n");
                    writer.write("    \"JDK\": " + data.get("JDK") + ",\n");
                    writer.write("    \"dataType\": \"" + data.get("dataType") + "\",\n");
                    writer.write("    \"testData\": \"" + data.get("testData").toString().replace("\"", "\\\"").replace("\n", "\\n") + "\"\n");
                    writer.write("  }");
                    first = false;
                }
            }
            writer.write("\n}\n");
        }
        System.out.println("Generated sizes.json with " + allSizes.size() + " benchmarks");
    }
    
    private static <T> int calculateSize(T object, Pickler<T> pickler) throws Exception {
        ByteBuffer buffer = ByteBuffer.allocate(pickler.maxSizeOf(object));
        int size = pickler.serialize(buffer, object);
        return size;
    }
    
    private static int calculateJdkSize(Object object) throws Exception {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(object);
        oos.close();
        return baos.toByteArray().length;
    }
}
