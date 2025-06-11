#!/bin/bash
# Check if the benchmark visualization server is running

# Check by PID
if [ -f /tmp/benchmark-server.pid ]; then
    PID=$(cat /tmp/benchmark-server.pid)
    if kill -0 $PID 2>/dev/null; then
        echo "Server running with PID: $PID"
    else
        echo "PID file exists but process $PID not running"
        rm /tmp/benchmark-server.pid
    fi
else
    echo "No PID file found"
fi

# Check port 8080
PORT_PROCESS=$(lsof -ti:8080 2>/dev/null)
if [ -n "$PORT_PROCESS" ]; then
    echo "Process using port 8080: $PORT_PROCESS"
    echo "Server URL: http://localhost:8080"
else
    echo "No process using port 8080"
fi

# Test server response
echo "Testing server response..."
curl -s -w "HTTP Status: %{http_code}\n" http://localhost:8080/ -o /dev/null