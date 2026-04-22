#!/usr/bin/env sh
set -e

# Function to catch shutdown signals
cleanup() {
    echo "Shutting down..."
    kill "$DOCKERD_PID"
    exit 0
}

# Trap SIGTERM and SIGINT
trap cleanup TERM INT

# Start Docker in background
dockerd-entrypoint.sh --storage-driver vfs &
DOCKERD_PID=$!

# Wait for Docker to be ready
while ! docker info >/dev/null 2>&1; do
    sleep 1
done

# Start your app
echo "Starting inference endpoint..."
cd /inference-endpoint
docker compose down
docker compose up -d

# Wait for the background process
wait "$DOCKERD_PID"
