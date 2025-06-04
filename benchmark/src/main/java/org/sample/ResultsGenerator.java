package org.sample;

import java.io.*;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

/**
 * Converts JMH benchmark output to NJSON format with size data.
 * Usage: mvn exec:java -Dexec.mainClass="org.sample.ResultsGenerator" -Dexec.args="/tmp/tree-benchmark-results.txt"
 */
public class ResultsGenerator {
    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            System.err.println("Usage: java ResultsGenerator <benchmark-results-file>");
            System.exit(1);
        }
        
        String inputFile = args[0];
        String outputFile = "results.njson";
        
        System.out.println("Converting " + inputFile + " to " + outputFile);
        
        // Size data from SizeCalculator measurements
        int nfpTreeSize = 1096;
        int jdkTreeSize = 1690;
        int ptbTreeSize = 0; // TODO: get from protobuf measurement
        
        // Primitive benchmark sizes (measured)
        int nfpPrimitivesSize = 34;
        int jdkPrimitivesSize = 177;
        
        String timestamp = Instant.now().toString();
        String comment = "Baseline TreeBenchmark results after buffer size fix - NFP working with 2KB buffer";
        
        try (BufferedReader reader = new BufferedReader(new FileReader(inputFile));
             PrintWriter writer = new PrintWriter(new FileWriter(outputFile))) {
            
            String line;
            while ((line = reader.readLine()) != null) {
                System.out.println("Checking line: " + line);
                if (line.startsWith("TreeBenchmark.") || line.startsWith("PrimitiveBenchmark.")) {
                    System.out.println("Found benchmark line: " + line);
                    String[] parts = line.split("\\s+");
                    String benchmark = parts[0];
                    String mode = parts[1]; // thrpt
                    int cnt = Integer.parseInt(parts[2]);
                    double score = Double.parseDouble(parts[3]);
                    double error = Double.parseDouble(parts[5]);
                    String units = parts[6];
                    
                    // Determine source and size
                    String src;
                    int size;
                    if (benchmark.contains("treeJdk")) {
                        src = "JDK";
                        size = jdkTreeSize;
                    } else if (benchmark.contains("treeNfp")) {
                        src = "NFP";
                        size = nfpTreeSize;
                    } else if (benchmark.contains("treeProtobuf")) {
                        src = "PTB";
                        size = ptbTreeSize;
                    } else if (benchmark.contains("primitivesJdk")) {
                        src = "JDK";
                        size = jdkPrimitivesSize;
                    } else if (benchmark.contains("primitivesNfp")) {
                        src = "NFP";
                        size = nfpPrimitivesSize;
                    } else {
                        continue; // Skip unknown benchmarks
                    }
                    
                    // Generate NJSON line
                    String njsonLine = String.format(
                        "{\"benchmark\":\"%s\",\"src\":\"%s\",\"mode\":\"%s\",\"cnt\":%d,\"score\":%.3f,\"error\":%.3f,\"units\":\"%s\",\"size\":%d,\"ts\":\"%s\",\"comment\":\"%s\"}",
                        benchmark, src, mode, cnt, score, error, units, size, timestamp, comment
                    );
                    
                    writer.println(njsonLine);
                    System.out.println("Generated: " + njsonLine);
                }
            }
        }
        
        System.out.println("Results written to " + outputFile);
    }
}