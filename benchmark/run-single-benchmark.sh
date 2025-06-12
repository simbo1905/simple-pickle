#!/bin/bash

# Quick single benchmark runner for testing
if [ $# -ne 1 ]; then
    echo "Usage: $0 <BenchmarkClass>"
    echo "Example: $0 SimpleWrite"
    exit 1
fi

BENCHMARK=$1
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
OUTPUT_FILE="results-${TIMESTAMP}.njson"

echo "Running ${BENCHMARK} benchmark..."

# Build and run benchmark with shorter iterations
mvn clean compile exec:java -Dexec.mainClass="org.openjdk.jmh.Main" \
    -Dexec.args="${BENCHMARK} -wi 2 -i 3 -f 1 -rf json -rff jmh-result-${TIMESTAMP}.json" -q

# Convert JMH JSON to NJSON
if [ -f "jmh-result-${TIMESTAMP}.json" ]; then
    echo "Converting results to NJSON format..."
    mvn exec:java -Dexec.mainClass="org.sample.ResultsGenerator" -Dexec.args="jmh-result-${TIMESTAMP}.json" -q
    
    if [ -f "results.njson" ]; then
        mv results.njson "${OUTPUT_FILE}"
        echo "Results saved to: ${OUTPUT_FILE}"
        cat "${OUTPUT_FILE}"
    else
        echo "Failed to generate NJSON output"
    fi
else
    echo "No JMH results file generated"
fi