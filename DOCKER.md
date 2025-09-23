# MetricFlow MCP Server Docker Setup

This repository provides a streamlined Docker setup for running MetricFlow as an MCP (Model Context Protocol) server.

## Quick Start

### Build and run directly
```bash
# Build the image
docker build -t datus-mf .

# Run MCP server
docker run -p 8080:8080 datus-mf

# Run with demo data
docker run -p 8080:8080 datus-mf demo

# Run with persistent storage
docker run -p 8080:8080 \
  -v metricflow_data:/root/.metricflow \
  -v datus_data:/root/.datus \
  datus-mf
```


## Available Endpoints

- **Health Check**: `GET /health`
- **MCP JSON-RPC**: `POST /mcp`
- **SSE Stream**: `GET /sse`

## MCP Tools Available

1. `list_metrics` - List all available metrics
2. `get_dimensions` - Get dimensions for metrics
3. `get_entities` - Get entities for metrics
4. `query_metrics` - Execute MetricFlow queries
5. `validate_configs` - Validate configurations
6. `get_dimension_values` - Get dimension values

## Container Commands

- `serve` (default) - Start MCP server
- `demo` - Setup demo data and start server
- `init` - Initialize configs only
- Any other command - Execute directly

## Configuration

All configurations are automatically initialized on container startup:
- MetricFlow config: `/root/.metricflow/config.yml`

Volumes persist configuration between container restarts.

## Testing MCP Server

### Health Check
```bash
curl http://localhost:8080/health
```

### List Available Tools
```bash
curl -X POST http://localhost:8080/mcp \
  -H "Content-Type: application/json" \
  -d '{"jsonrpc": "2.0", "id": 1, "method": "tools/list"}'
```

### Execute a Tool
```bash
curl -X POST http://localhost:8080/mcp \
  -H "Content-Type: application/json" \
  -d '{
    "jsonrpc": "2.0",
    "id": 1,
    "method": "tools/call",
    "params": {
      "name": "list_metrics",
      "arguments": {}
    }
  }'
```

## Troubleshooting

### Check container logs
```bash
docker logs mcp-metricflow
```

### Reset configuration
```bash
# Remove volumes and restart
docker volume rm metricflow_data datus_data
docker run -p 8080:8080 datus-mf
```