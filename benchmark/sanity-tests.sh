#!/bin/bash
# Sanity tests for benchmark visualization server
# Must pass before attempting UI tests

cd "$(dirname "$0")"

echo "=== Benchmark Server Sanity Tests ==="

# Test 1: Server info endpoint
echo "1. Testing /info endpoint..."
INFO_RESPONSE=$(curl -s http://localhost:8080/info)
if echo "$INFO_RESPONSE" | grep -q "NFP Benchmark Visualizer"; then
    echo "   ✓ Server info endpoint working"
else
    echo "   ✗ Server info endpoint failed"
    echo "   Response: $INFO_RESPONSE"
    exit 1
fi

# Test 2: Search endpoint
echo "2. Testing /api/search endpoint..."
SEARCH_RESPONSE=$(curl -s "http://localhost:8080/api/search")
if [ $? -eq 0 ]; then
    FILE_COUNT=$(echo "$SEARCH_RESPONSE" | grep -o '\.njson' | wc -l)
    echo "   ✓ Search endpoint working (found $FILE_COUNT result files)"
else
    echo "   ✗ Search endpoint failed"
    exit 1
fi

# Test 3: Results endpoint  
echo "3. Testing /api/results endpoint..."
RESULTS_RESPONSE=$(curl -s "http://localhost:8080/api/results")
if echo "$RESULTS_RESPONSE" | grep -q '"filename"'; then
    FILENAME=$(echo "$RESULTS_RESPONSE" | grep -o '"filename":"[^"]*"' | cut -d'"' -f4)
    DATA_COUNT=$(echo "$RESULTS_RESPONSE" | grep -o '"benchmark":' | wc -l)
    echo "   ✓ Results endpoint working (file: $FILENAME, $DATA_COUNT records)"
else
    echo "   ✗ Results endpoint failed"
    echo "   Response: $RESULTS_RESPONSE"
    exit 1
fi

# Test 4: Specific file endpoint
echo "4. Testing /api/file endpoint..."
if [ "$FILENAME" != "no-files-found" ]; then
    FILE_RESPONSE=$(curl -s "http://localhost:8080/api/file?name=$FILENAME")
    if echo "$FILE_RESPONSE" | grep -q '"filename"'; then
        echo "   ✓ File endpoint working (loaded $FILENAME)"
    else
        echo "   ✗ File endpoint failed"
        exit 1
    fi
else
    echo "   ⚠ No data files available - file endpoint not tested"
fi

echo ""
echo "=== Sanity Tests Summary ==="
if [ "$DATA_COUNT" -gt 0 ]; then
    echo "✓ All endpoints working with $DATA_COUNT benchmark records"
    echo "✓ Ready for UI testing"
else
    echo "⚠ Server working but no benchmark data available"
    echo "⚠ UI tests will show empty state"
fi