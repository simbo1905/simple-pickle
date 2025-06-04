#!/usr/bin/env python3

import argparse
import subprocess
import json
import sys
from datetime import datetime
import os

def run_benchmark(quick=False, tests=None):
    """Run JMH benchmark with specified parameters"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = f"jmh-result-{timestamp}.json"
    njson_file = f"results-{timestamp}.njson"
    
    # Build base command
    cmd = ["java", "-jar", "target/benchmarks.jar"]
    
    if tests:
        # Run specific tests
        cmd.extend(tests)
    
    if quick:
        # Quick test parameters
        cmd.extend(["-f", "1", "-wi", "1", "-i", "1"])
    
    # Add JSON output
    cmd.extend(["-rf", "json", "-rff", output_file])
    
    print(f"Running: {' '.join(cmd)}")
    print(f"Output file: {output_file}")
    
    try:
        # Run the benchmark
        result = subprocess.run(cmd, check=True, capture_output=True, text=True)
        print("Benchmark completed successfully")
        
        # Process results
        process_results(output_file, njson_file)
        
        return output_file, njson_file
        
    except subprocess.CalledProcessError as e:
        print(f"Benchmark failed with exit code {e.returncode}")
        print(f"stdout: {e.stdout}")
        print(f"stderr: {e.stderr}")
        sys.exit(1)

def get_sizes():
    """Get serialization sizes by running the size calculator"""
    print("Calculating serialization sizes...")
    try:
        result = subprocess.run([
            "mvn", "exec:java", 
            "-Dexec.mainClass=org.sample.SizeCalculator",
            "-q"
        ], check=True, capture_output=True, text=True)
        
        # Parse sizes from output - assuming format like "NFP:123,JDK:456,PTB:789"
        sizes = {}
        for line in result.stdout.strip().split('\n'):
            if ':' in line and ',' in line:
                for pair in line.split(','):
                    if ':' in pair:
                        src, size = pair.strip().split(':')
                        sizes[src] = int(size)
        
        return sizes
    except subprocess.CalledProcessError as e:
        print(f"Size calculation failed: {e}")
        return {}

def process_results(json_file, njson_file):
    """Convert JMH JSON to NJSON format with sizes"""
    print(f"Processing {json_file} to {njson_file}")
    
    # Get sizes
    sizes = get_sizes()
    
    # Load JMH results
    with open(json_file, 'r') as f:
        jmh_data = json.load(f)
    
    timestamp = datetime.now().isoformat()
    comment = "Automated benchmark run"
    
    # Convert to NJSON
    with open(njson_file, 'w') as f:
        for result in jmh_data:
            benchmark = result['benchmark']
            
            # Determine source from benchmark name
            src = "UNKNOWN"
            size = 0
            
            if 'Jdk' in benchmark or 'jdk' in benchmark:
                src = "JDK"
            elif 'Nfp' in benchmark or 'nfp' in benchmark:
                src = "NFP"
            elif 'Protobuf' in benchmark or 'protobuf' in benchmark:
                src = "PTB"
            
            # Get size if available
            if src in sizes:
                size = sizes[src]
            
            # Create NJSON record
            record = {
                "benchmark": benchmark,
                "src": src,
                "mode": result['mode'],
                "score": result['primaryMetric']['score'],
                "error": result['primaryMetric']['scoreError'],
                "units": result['primaryMetric']['scoreUnit'],
                "size": size,
                "ts": timestamp,
                "comment": comment
            }
            
            f.write(json.dumps(record) + '\n')
    
    print(f"NJSON results written to {njson_file}")

def main():
    parser = argparse.ArgumentParser(description='Run JMH benchmarks and process results')
    parser.add_argument('-q', '--quick', action='store_true', 
                       help='Run quick test (1 fork, 1 warmup, 1 iteration)')
    parser.add_argument('tests', nargs='*', 
                       help='Specific tests to run (default: all)')
    
    args = parser.parse_args()
    
    # Check if we're in the right directory
    if not os.path.exists('target/benchmarks.jar'):
        print("Error: target/benchmarks.jar not found. Run 'mvn clean verify' first.")
        sys.exit(1)
    
    output_file, njson_file = run_benchmark(args.quick, args.tests)
    print(f"\nCompleted successfully:")
    print(f"  JMH JSON: {output_file}")
    print(f"  NJSON:    {njson_file}")

if __name__ == "__main__":
    main()