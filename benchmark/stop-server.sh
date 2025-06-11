#!/bin/bash
# Stop the benchmark visualization server

# Kill by PID if available
if [ -f /tmp/benchmark-server.pid ]; then
    PID=$(cat /tmp/benchmark-server.pid)
    if kill -0 $PID 2>/dev/null; then
        echo "Stopping server with PID: $PID"
        kill $PID
        rm /tmp/benchmark-server.pid
    else
        echo "Server with PID $PID not running"
        rm /tmp/benchmark-server.pid
    fi
fi

# Kill any remaining processes on port 8080
lsof -ti:8080 | xargs kill -9 2>/dev/null && echo "Killed remaining processes on port 8080" || echo "No processes found on port 8080"

echo "Server stopped"