#!/bin/bash

# Docker initialization script for MetricFlow MCP Server
# This script initializes all configurations and starts the MCP server

set -e

echo "=== MetricFlow MCP Server Initialization ==="

# Create directories if they don't exist
mkdir -p /root/.metricflow/semantic_models
mkdir -p /root/.datus/conf

# Configuration will be created by setup command

# Function to start both MCP servers
start_mcp_servers() {
    echo "Starting filesystem MCP server on port 8081..."
    FILESYSTEM_MCP_PORT=8081 FILESYSTEM_ROOT_PATH="$MF_MODEL_PATH" python -m mcp_metricflow.filesystem_server "$MF_MODEL_PATH" &
    FILESYSTEM_PID=$!

    echo "Starting MetricFlow MCP server on port 8080..."
    python -m mcp_metricflow.server &
    METRICFLOW_PID=$!

    # Trap signals to gracefully shutdown both processes
    trap 'kill $FILESYSTEM_PID $METRICFLOW_PID; wait $FILESYSTEM_PID $METRICFLOW_PID' SIGTERM SIGINT

    echo "Both MCP servers started:"
    echo "  - Filesystem MCP: http://0.0.0.0:8081"
    echo "  - MetricFlow MCP: http://0.0.0.0:8080"

    # Wait for both processes
    wait $FILESYSTEM_PID $METRICFLOW_PID
}

# Handle different startup modes
case "${1:-serve}" in
    "demo")
        echo "Setting up demo configuration..."

        # Use datus-mf setup to generate proper configuration
        echo "Running datus-mf setup to create demo configuration..."
        datus-mf setup --demo || echo "Setup encountered issues, continuing..."

        echo "Starting MCP servers with demo configuration..."
        start_mcp_servers
        ;;
    "serve")
        echo "Starting MCP servers..."
        start_mcp_servers
        ;;
    "init")
        echo "Configuration initialized. Available commands:"
        echo "  docker run ... demo     # Start with demo data and both MCP servers"
        echo "  docker run ... serve    # Start both MCP servers"
        echo "  docker exec ... datus-mf setup --demo"
        echo "  docker exec ... datus-mf setup --dialect <dialect>"
        echo ""
        echo "Available MCP servers:"
        echo "  - MetricFlow MCP: http://localhost:8080/mcp"
        echo "  - Filesystem MCP: http://localhost:8081"
        exit 0
        ;;
    *)
        echo "Executing custom command: $*"
        exec "$@"
        ;;
esac