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

# Test 2: JMH search endpoint
echo "2. Testing /api/search?type=jmh endpoint..."
SEARCH_RESPONSE=$(curl -s "http://localhost:8080/api/search?type=jmh")
if [ $? -eq 0 ]; then
    FILE_COUNT=$(echo "$SEARCH_RESPONSE" | grep -o 'jmh-result-' | wc -l)
    echo "   ✓ JMH search endpoint working (found $FILE_COUNT JMH files)"
else
    echo "   ✗ JMH search endpoint failed"
    exit 1
fi

# Test 3: JMH results endpoint  
echo "3. Testing /api/jmh-results endpoint..."
JMH_RESPONSE=$(curl -s "http://localhost:8080/api/jmh-results")
if echo "$JMH_RESPONSE" | grep -q '"filename"'; then
    JMH_FILENAME=$(echo "$JMH_RESPONSE" | grep -o '"filename":"[^"]*"' | cut -d'"' -f4)
    JMH_COUNT=$(echo "$JMH_RESPONSE" | grep -o '"benchmark":' | wc -l)
    echo "   ✓ JMH results endpoint working (file: $JMH_FILENAME, $JMH_COUNT records)"
else
    echo "   ✗ JMH results endpoint failed"
    echo "   Response: $JMH_RESPONSE"
    exit 1
fi

# Test 4: Sizes endpoint
echo "4. Testing /api/sizes endpoint..."
SIZES_RESPONSE=$(curl -s "http://localhost:8080/api/sizes")
if echo "$SIZES_RESPONSE" | grep -q '"filename"'; then
    SIZES_FILENAME=$(echo "$SIZES_RESPONSE" | grep -o '"filename":"[^"]*"' | cut -d'"' -f4)
    SIZES_COUNT=$(echo "$SIZES_RESPONSE" | grep -o '"NFP":' | wc -l)
    echo "   ✓ Sizes endpoint working (file: $SIZES_FILENAME, $SIZES_COUNT test types)"
else
    echo "   ✗ Sizes endpoint failed"
    echo "   Response: $SIZES_RESPONSE"
    exit 1
fi

# Test 5: JMH file endpoint
echo "5. Testing /api/jmh-file endpoint..."
if [ "$JMH_FILENAME" != "no-jmh-files-found" ]; then
    FILE_RESPONSE=$(curl -s "http://localhost:8080/api/jmh-file?name=$JMH_FILENAME")
    if echo "$FILE_RESPONSE" | grep -q '"filename"'; then
        echo "   ✓ JMH file endpoint working (loaded $JMH_FILENAME)"
    else
        echo "   ✗ JMH file endpoint failed"
        exit 1
    fi
else
    echo "   ⚠ No JMH files available - file endpoint not tested"
fi

echo ""
echo "=== Sanity Tests Summary ==="
if [ "$JMH_COUNT" -gt 0 ] && [ "$SIZES_COUNT" -gt 0 ]; then
    echo "✓ All NEW endpoints working with $JMH_COUNT JMH records and $SIZES_COUNT size types"
    echo "✓ Ready for UI testing with new architecture"
else
    echo "⚠ Server working but missing JMH data ($JMH_COUNT) or sizes data ($SIZES_COUNT)"
    echo "⚠ UI tests will show empty state"
fi