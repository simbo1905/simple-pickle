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
            
            // Output in parseable format
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
}
