#!/bin/bash

# Generate timestamp for unique filenames
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
OUTPUT_FILE="jmh-result-${TIMESTAMP}.json"
NJSON_FILE="results-${TIMESTAMP}.njson"

echo "Running benchmark with output: ${OUTPUT_FILE}"

# Run JMH benchmark with JSON output
java -jar target/benchmarks.jar -rf json -rff "${OUTPUT_FILE}"

if [ $? -eq 0 ]; then
    echo "Benchmark completed successfully"
    echo "Processing results to NJSON format: ${NJSON_FILE}"
    
    # Process the JSON results to NJSON format
    mvn exec:java -Dexec.mainClass="org.sample.BenchmarkProcessor" -Dexec.args="${OUTPUT_FILE} ${NJSON_FILE}"
    
    if [ $? -eq 0 ]; then
        echo "Results processed successfully to ${NJSON_FILE}"
        echo "Raw JMH JSON: ${OUTPUT_FILE}"
    else
        echo "Failed to process results"
        exit 1
    fi
else
    echo "Benchmark failed"
    exit 1
fi