#!/bin/bash

# Docker initialization script for MetricFlow MCP Server
# This script initializes all configurations and starts the MCP server

set -e

echo "=== MetricFlow MCP Server Initialization ==="

# Create directories if they don't exist
mkdir -p /root/.metricflow/semantic_models
mkdir -p /root/.datus/conf

# Configuration will be created by setup command

# Create basic Datus Agent config if it doesn't exist
if [ ! -f "/root/.datus/conf/agent.yml" ]; then
    echo "Creating Datus Agent configuration..."
    cat > /root/.datus/conf/agent.yml << EOF
namespace: local_metricflow
EOF
fi

# Handle different startup modes
case "${1:-serve}" in
    "demo")
        echo "Setting up demo configuration..."

        # Use datus-mf setup to generate proper configuration
        echo "Running datus-mf setup to create demo configuration..."
        datus-mf setup --demo || echo "Setup encountered issues, continuing..."

        echo "Starting MCP server with demo configuration..."
        exec python -m mcp_metricflow.server
        ;;
    "serve")
        echo "Starting MCP server..."
        exec python -m mcp_metricflow.server
        ;;
    "init")
        echo "Configuration initialized. Available commands:"
        echo "  docker run ... demo     # Start with demo data"
        echo "  docker run ... serve    # Start MCP server"
        echo "  docker exec ... datus-mf setup --demo"
        echo "  docker exec ... datus-mf setup --dialect <dialect>"
        exit 0
        ;;
    *)
        echo "Executing custom command: $*"
        exec "$@"
        ;;
esac