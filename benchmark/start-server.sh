  #!/bin/bash
# Start the benchmark visualization server

cd "$(dirname "$0")"

SKIP_REBUILD=false
if [[ "$1" == "-skipRebuild" ]]; then
    SKIP_REBUILD=true
    echo "Skip rebuild mode enabled"
fi

# Stop existing server if running
if [ -f /tmp/benchmark-server.pid ]; then
    echo "Stopping existing server..."
    ./stop-server.sh
else
    # Check if port 8080 is in use and warn
    PORT_PID=$(lsof -ti:8080 2>/dev/null)
    if [ ! -z "$PORT_PID" ]; then
        echo "Warning: Port 8080 is in use by PID $PORT_PID (not our server)"
        echo "Please stop that process or use a different port"
        exit 1
    fi
fi

# Run tests unless skipping rebuild
if [ "$SKIP_REBUILD" = false ]; then
    echo "Running Jest tests..."
    cd assets
    npm test > /tmp/jest-output.log 2>&1
    TEST_RESULT=$?
    cd ..
    if [ $TEST_RESULT -ne 0 ]; then
        echo "Tests failed - aborting server start"
        cat /tmp/jest-output.log
        exit 1
    fi
    echo "Tests passed - starting server"
fi

# Start server directly from source (Java 21 feature)
echo "Starting server with data directory: ."
java Server.java . > /tmp/benchmark-server.log 2>&1 &
SERVER_PID=$!

# Save PID for stop script
echo $SERVER_PID > /tmp/benchmark-server.pid

# Wait for server to be ready
echo "Waiting for server to start..."
for i in {1..4}; do
    if curl -s http://localhost:8080/info > /dev/null 2>&1; then
        echo "Server ready at: http://localhost:8080"
        echo "Server PID: $SERVER_PID"
        echo "Log file: /tmp/benchmark-server.log"
        echo "To stop server: ./stop-server.sh"
        exit 0
    fi
    sleep 1
done

echo "Server failed to start - check /tmp/benchmark-server.log"
exit 1
