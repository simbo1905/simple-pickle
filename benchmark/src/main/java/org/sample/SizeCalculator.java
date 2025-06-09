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
        Map<String, Map<String, Integer>> allSizes = new LinkedHashMap<>();
        
        // SimpleBenchmark
        SimpleBenchmark.TestRecord simpleRecord = new SimpleBenchmark.TestRecord("Test Name", 42, 123456789L, true);
        allSizes.put("SimpleBenchmark", Map.of(
            "NFP", calculateSize(simpleRecord, Pickler.forClass(SimpleBenchmark.TestRecord.class)),
            "JDK", calculateJdkSize(simpleRecord)
        ));
        
        // PrimitiveBenchmark
        PrimitiveBenchmark.PrimitiveRecord primitiveRecord = new PrimitiveBenchmark.PrimitiveRecord(
            true, (byte)42, (short)1337, 'X', 987654321, 1234567890123456789L, 3.14159f, 2.718281828459045
        );
        allSizes.put("PrimitiveBenchmark", Map.of(
            "NFP", calculateSize(primitiveRecord, Pickler.forClass(PrimitiveBenchmark.PrimitiveRecord.class)),
            "JDK", calculateJdkSize(primitiveRecord)
        ));
        
        // StringBenchmark
        StringBenchmark.StringRecord stringRecord = new StringBenchmark.StringRecord(
            "Hello",
            "The quick brown fox jumps over the lazy dog",
            "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua."
        );
        allSizes.put("StringBenchmark", Map.of(
            "NFP", calculateSize(stringRecord, Pickler.forClass(StringBenchmark.StringRecord.class)),
            "JDK", calculateJdkSize(stringRecord)
        ));
        
        // ArrayBenchmark
        ArrayBenchmark.ArrayRecord arrayRecord = new ArrayBenchmark.ArrayRecord(
            new int[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
            new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
            new String[]{"one", "two", "three", "four", "five"}
        );
        allSizes.put("ArrayBenchmark", Map.of(
            "NFP", calculateSize(arrayRecord, Pickler.forClass(ArrayBenchmark.ArrayRecord.class)),
            "JDK", calculateJdkSize(arrayRecord)
        ));
        
        // ListBenchmark
        ListBenchmark.ListRecord listRecord = new ListBenchmark.ListRecord(
            List.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10),
            List.of("one", "two", "three", "four", "five"),
            List.of(List.of("a", "b", "c"), List.of("d", "e", "f"), List.of("g", "h", "i"))
        );
        allSizes.put("ListBenchmark", Map.of(
            "NFP", calculateSize(listRecord, Pickler.forClass(ListBenchmark.ListRecord.class)),
            "JDK", calculateJdkSize(listRecord)
        ));
        
        // OptionalBenchmark - NFP only (Optional not Serializable)
        OptionalBenchmark.OptionalRecord optionalRecord = new OptionalBenchmark.OptionalRecord(
            Optional.of("Hello Optional"), Optional.of(42), Optional.empty(), Optional.empty()
        );
        allSizes.put("OptionalBenchmark", Map.of(
            "NFP", calculateSize(optionalRecord, Pickler.forClass(OptionalBenchmark.OptionalRecord.class)),
            "JDK", -1  // Not applicable
        ));
        
        // Output sizes in parseable format for each benchmark
        for (Map.Entry<String, Map<String, Integer>> entry : allSizes.entrySet()) {
            String benchmark = entry.getKey();
            Map<String, Integer> sizes = entry.getValue();
            if (sizes.get("JDK") > 0) {
                System.out.println(benchmark + "_NFP:" + sizes.get("NFP") + "," + benchmark + "_JDK:" + sizes.get("JDK"));
            }
        }
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
