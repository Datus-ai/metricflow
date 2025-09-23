# MetricFlow MCP Servers

A collection of Model Context Protocol (MCP) servers that provide MetricFlow functionality and filesystem access using JSON-RPC protocol.

## Overview

This package includes two MCP servers that provide a standardized interface for LLMs:

### MetricFlow MCP Server
- **Metric Queries**: Execute MetricFlow queries with structured parameters
- **Configuration Management**: Access and validate MetricFlow configurations
- **Validation Tools**: Built-in configuration and health checks

### Filesystem MCP Server
- **File Operations**: Read, write, and manage files in the semantic models directory
- **Directory Management**: List, create, and organize project files
- **Secure Access**: Safe file operations within allowed directories

### Common Features
- **Standard Protocol**: JSON-RPC 2.0 compliant MCP implementation
- **Health Monitoring**: Built-in health checks and status reporting
- **Docker Support**: Ready-to-use containerized deployment

## Quick Start

### 1. Install Dependencies

```bash
# Install with poetry (recommended)
poetry install

# Or with pip
pip install -e .
```

### 2. Setup MetricFlow

```bash
# Setup demo environment
datus-mf setup --demo

# Or setup for production
datus-mf setup --dialect snowflake
```

### 3. Start MCP Servers

```bash
# Start MetricFlow MCP server (port 8080)
mcp-metricflow serve

# Start Filesystem MCP server (port 8081)
python -m mcp_metricflow.filesystem_server /path/to/semantic/models

# Or with custom configuration
mcp-metricflow serve --host 0.0.0.0 --port 8080
FILESYSTEM_MCP_PORT=8081 FILESYSTEM_ROOT_PATH=/path/to/models python -m mcp_metricflow.filesystem_server
```

### 4. Test Connection

```bash
# Test MetricFlow MCP endpoint
curl -X POST http://localhost:8080/mcp \
  -H "Content-Type: application/json" \
  -d '{"jsonrpc": "2.0", "method": "initialize", "id": 1, "params": {}}'

# Test Filesystem MCP endpoint
curl -X POST http://localhost:8081/mcp \
  -H "Content-Type: application/json" \
  -d '{"jsonrpc": "2.0", "method": "initialize", "id": 1, "params": {}}'
```

## Docker Deployment

### Build Docker Image

```bash
# Build the image
docker build -t metricflow-mcp .
```

### Quick Start

```bash
# Start both MCP servers
docker run -p 8080:8080 -p 8081:8081 metricflow-mcp serve

# Start demo server with auto-setup
docker run -p 8080:8080 -p 8081:8081 metricflow-mcp demo

# Access endpoints
# - MetricFlow MCP: http://localhost:8080/mcp
# - Filesystem MCP: http://localhost:8081/mcp
# - MetricFlow Health: http://localhost:8080/health
# - Filesystem Health: http://localhost:8081/health
```

### Production Deployment

```bash
# Run with persistent volumes
docker run -d \
  -p 8080:8080 \
  -p 8081:8081 \
  -v $(pwd)/data:/root/.metricflow \
  --name metricflow-mcp \
  metricflow-mcp serve
```

## MCP Tools

### MetricFlow MCP Server Tools

#### Query Tools
- **`list_metrics`**: List all available metrics
- **`get_dimensions`**: Get dimensions for MetricFlow project
- **`get_entities`**: Get entities for MetricFlow project
- **`query_metrics`**: Execute MetricFlow queries with structured parameters
- **`get_dimension_values`**: Get possible values for a specific dimension

#### Management Tools
- **`validate_configs`**: Validate MetricFlow configuration and semantic models

### Filesystem MCP Server Tools

#### File Operations
- **`read_file`**: Read contents of a text file
- **`write_file`**: Write content to a file
- **`file_info`**: Get information about a file or directory

#### Directory Operations
- **`list_directory`**: List contents of a directory
- **`create_directory`**: Create a new directory
- **`delete_file`**: Delete a file or directory

## JSON-RPC Protocol

### Server Endpoints

- **MetricFlow MCP**: `http://localhost:8080/mcp`
- **Filesystem MCP**: `http://localhost:8081/mcp`

### Connection

Connect to either MCP endpoint using JSON-RPC 2.0:

```bash
# Initialize MetricFlow MCP
curl -X POST http://localhost:8080/mcp \
  -H "Content-Type: application/json" \
  -d '{"jsonrpc": "2.0", "method": "initialize", "id": 1, "params": {}}'

# Initialize Filesystem MCP
curl -X POST http://localhost:8081/mcp \
  -H "Content-Type: application/json" \
  -d '{"jsonrpc": "2.0", "method": "initialize", "id": 1, "params": {}}'
```

### Available Methods

- **`initialize`**: Initialize MCP connection
- **`notifications/initialized`**: Notify initialization complete
- **`tools/list`**: List available tools for the specific server
- **`tools/call`**: Call a specific tool

### Example Tool Calls

```bash
# List MetricFlow metrics
curl -X POST http://localhost:8080/mcp \
  -H "Content-Type: application/json" \
  -d '{
    "jsonrpc": "2.0",
    "method": "tools/call",
    "id": 1,
    "params": {
      "name": "list_metrics",
      "arguments": {}
    }
  }'

# List directory contents
curl -X POST http://localhost:8081/mcp \
  -H "Content-Type: application/json" \
  -d '{
    "jsonrpc": "2.0",
    "method": "tools/call",
    "id": 1,
    "params": {
      "name": "list_directory",
      "arguments": {"path": "."}
    }
  }'
```


## Configuration

### Environment Variables

#### MetricFlow MCP Server
- **`MF_PROJECT_DIR`**: MetricFlow project directory (default: `~/.metricflow`)
- **`MF_VERBOSE`**: Enable verbose logging (default: `true`)
- **`MF_MODEL_PATH`**: Path to semantic models (default: `~/.metricflow/semantic_models`)
- **`MCP_HOST`**: MCP server host (default: `0.0.0.0`)
- **`MCP_PORT`**: MCP server port (default: `8080`)

#### Filesystem MCP Server
- **`FILESYSTEM_ROOT_PATH`**: Root directory for filesystem access (default: `$MF_MODEL_PATH`)
- **`FILESYSTEM_MCP_HOST`**: Filesystem MCP server host (default: `0.0.0.0`)
- **`FILESYSTEM_MCP_PORT`**: Filesystem MCP server port (default: `8081`)

### Docker Environment

```yaml
environment:
  - MF_PROJECT_DIR=/root/.metricflow
  - MF_VERBOSE=true
  - MF_MODEL_PATH=/root/.metricflow/semantic_models
  - MCP_HOST=0.0.0.0
  - MCP_PORT=8080
```

## Health Monitoring

### Health Check Endpoints

```bash
# MetricFlow MCP health
curl http://localhost:8080/health

# Filesystem MCP health
curl http://localhost:8081/health
```

Response format:
```json
{
  "status": "healthy"
}
```

### Docker Health Checks

```bash
# Check MetricFlow MCP server
docker exec metricflow-mcp curl -f http://localhost:8080/health

# Check Filesystem MCP server
docker exec metricflow-mcp curl -f http://localhost:8081/health

# Check both servers in one command
docker exec metricflow-mcp sh -c 'curl -f http://localhost:8080/health && curl -f http://localhost:8081/health'
```

## Troubleshooting

### Common Issues

1. **MCP server won't start**
   ```bash
   # Check MetricFlow setup
   mcp-metricflow test
   datus-mf status
   ```

2. **MCP connection fails**
   ```bash
   # Check if servers are running
   curl http://localhost:8080/health  # MetricFlow
   curl http://localhost:8081/health  # Filesystem

   # Test MCP endpoints
   curl -X POST http://localhost:8080/mcp \
     -H "Content-Type: application/json" \
     -d '{"jsonrpc": "2.0", "method": "initialize", "id": 1}'

   curl -X POST http://localhost:8081/mcp \
     -H "Content-Type: application/json" \
     -d '{"jsonrpc": "2.0", "method": "initialize", "id": 1}'

   # Check logs
   docker logs metricflow-mcp
   ```

3. **Query execution errors**
   ```bash
   # Validate MetricFlow configs
   mf validate-configs

   # Check database connection
   mf health-checks
   ```

### Debug Mode

```bash
# Start with verbose logging
mcp-metricflow serve --verbose

# Test MCP connections
curl -X POST http://localhost:8080/mcp \
  -H "Content-Type: application/json" \
  -d '{"jsonrpc": "2.0", "method": "tools/list", "id": 1}'

curl -X POST http://localhost:8081/mcp \
  -H "Content-Type: application/json" \
  -d '{"jsonrpc": "2.0", "method": "tools/list", "id": 1}'
```

## Development

### Project Structure

```
mcp_metricflow/
├── __init__.py          # Package initialization
├── server.py            # MetricFlow MCP server implementation
├── filesystem_server.py # Filesystem MCP server implementation
└── cli.py               # Command-line interface
```

### Adding New Tools

Tools can be added to either server by implementing the appropriate MCP tool handlers. Refer to the existing implementations in `server.py` and `filesystem_server.py` for examples.