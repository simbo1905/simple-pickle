// Size calculator for actual benchmark data
package org.sample;

import java.io.*;
import java.nio.charset.StandardCharsets;
import io.github.simbo1905.no.framework.Pickler;

// Import actual benchmark records
import org.sample.TreeNode;
import org.sample.TreeBenchmark;
import org.sample.PrimitiveBenchmark;

public class SizeCalculator {
    public static void main(String[] args) throws Exception {
        System.out.println("=== Actual Benchmark Data Size Analysis ===");
        System.out.println();
        
        // Calculate Tree benchmark sizes
        calculateTreeSizes();
        System.out.println();
        
        // Calculate Primitive benchmark sizes (skip Paxos due to enum bug)
        calculatePrimitiveSizes();
        System.out.println();
        
        // NOTE: Paxos benchmark skipped due to enum serialization bug
        System.out.println("--- Paxos Benchmark Sizes ---");
        System.out.println("SKIPPED: Enum serialization bug - Cannot serialize NoOperation.NOOP");
    }
    
    private static void calculateTreeSizes() throws Exception {
        System.out.println("--- Tree Benchmark Sizes ---");
        
        // Create the actual tree used in TreeBenchmark
        TreeNode rootNode = TreeBenchmark.createBalancedTree(100);
        System.out.println("Tree nodes: 100 (balanced tree)");
        
        // NFP size
        final Pickler<TreeNode> treePickler = Pickler.of(TreeNode.class);
        int nfpSize;
        try (final var writeBuffer = treeByteBuffer.allocate(8192)) { // Large buffer to avoid overflow
            nfpSize = treePickler.serialize(writeBuffer, rootNode);
        }
        
        // JDK size
        ByteArrayOutputStream jdkOut = new ByteArrayOutputStream();
        try (ObjectOutputStream oos = new ObjectOutputStream(jdkOut)) {
            oos.writeObject(rootNode);
        }
        int jdkSize = jdkOut.size();
        
        System.out.println("NFP Tree size: " + nfpSize + " bytes");
        System.out.println("JDK Tree size: " + jdkSize + " bytes");
        
        int maxTreeSize = Math.max(nfpSize, jdkSize);
        int recommendedTreeBuffer = ((maxTreeSize / 1024) + 1) * 1024; // Round up to nearest 1K
        System.out.println("Recommended Tree buffer: " + recommendedTreeBuffer + " bytes");
    }
    
    
    private static void calculatePrimitiveSizes() throws Exception {
        System.out.println("--- Primitive Benchmark Sizes ---");
        
        // Use the actual data from PrimitiveBenchmark
        final PrimitiveBenchmark.AllPrimitives testData = new PrimitiveBenchmark.AllPrimitives(
            true, (byte)42, (short)1000, 'A', 123456, 9876543210L, 3.14f, 2.71828
        );
        System.out.println("AllPrimitives record: " + testData);
        
        // NFP size
        final Pickler<PrimitiveBenchmark.AllPrimitives> primitivePickler = Pickler.of(PrimitiveBenchmark.AllPrimitives.class);
        int nfpSize;
        try (final var writeBuffer = primitiveByteBuffer.allocate(1024)) { // Safe buffer size
            nfpSize = primitivePickler.serialize(writeBuffer, testData);
        }
        
        // JDK size
        ByteArrayOutputStream jdkOut = new ByteArrayOutputStream();
        try (ObjectOutputStream oos = new ObjectOutputStream(jdkOut)) {
            oos.writeObject(testData);
        }
        int jdkSize = jdkOut.size();
        
        System.out.println("NFP Primitives size: " + nfpSize + " bytes");
        System.out.println("JDK Primitives size: " + jdkSize + " bytes");
        
        int maxPrimitivesSize = Math.max(nfpSize, jdkSize);
        int recommendedPrimitivesBuffer = ((maxPrimitivesSize / 256) + 1) * 256; // Round up to nearest 256 bytes
        System.out.println("Recommended Primitives buffer: " + recommendedPrimitivesBuffer + " bytes");
    }
}
