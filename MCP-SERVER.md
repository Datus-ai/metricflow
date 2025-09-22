# MetricFlow MCP Server

A Model Context Protocol (MCP) server that wraps MetricFlow functionality and exposes it via tools and resources with Server-Sent Events (SSE) support.

## Overview

The MetricFlow MCP Server provides a standardized interface for LLMs to interact with MetricFlow, enabling:

- **Metric Queries**: Execute MetricFlow queries with structured parameters
- **Configuration Management**: Access and validate MetricFlow configurations
- **Real-time Communication**: SSE support for streaming query results
- **Health Monitoring**: Built-in health checks and status reporting

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

### 3. Start MCP Server

```bash
# Start the MCP server
mcp-metricflow serve

# Or with custom host/port
mcp-metricflow serve --host 0.0.0.0 --port 8080
```

### 4. Test Connection

```bash
# Test MetricFlow integration
mcp-metricflow test

# Access SSE endpoint
curl -N http://localhost:8080/sse
```

## Docker Deployment

### Quick Start with Docker Compose

```bash
# Build and start MCP server
docker-compose -f docker-compose.mcp.yml up mcp-metricflow

# Start demo server with auto-setup
docker-compose -f docker-compose.mcp.yml up mcp-metricflow-demo

# Access endpoints
# - SSE: http://localhost:8080/sse
# - Health: http://localhost:8080/sse/health
```

### Production Deployment

```bash
# Start with nginx load balancer
docker-compose -f docker-compose.mcp.yml up

# Access via proxy
# - SSE: http://localhost/sse
# - Health: http://localhost/health
```

## MCP Tools

The server exposes the following tools for LLM interaction:

### Query Tools

- **`list_metrics`**: List all available metrics
- **`list_dimensions`**: List all available dimensions
- **`query_metrics`**: Execute MetricFlow queries with structured parameters
- **`get_sql_for_query`**: Get SQL for a query without executing

### Management Tools

- **`validate_configs`**: Validate MetricFlow configuration and semantic models
- **`health_check`**: Perform MetricFlow health checks
- **`explain_metric`**: Get detailed information about a specific metric

## MCP Resources

- **`metricflow/config`**: Get MetricFlow configuration
- **`metricflow/status`**: Get MetricFlow status and health information

## SSE Protocol

### Connection

Connect to the SSE endpoint:

```bash
curl -N -H "Accept: text/event-stream" http://localhost:8080/sse
```

### Event Types

- **`connection`**: Connection established with server capabilities
- **`heartbeat`**: Keep-alive events every 30 seconds
- **`query_started`**: Query execution started
- **`query_results`**: Query results available
- **`query_error`**: Query execution error
- **`error`**: General connection error

### Example Events

```
event: connection
data: {
data:   "type": "connection_established",
data:   "connection_id": "conn_1234567890",
data:   "server": "MetricFlow MCP Server",
data:   "capabilities": {
data:     "tools": ["list_metrics", "query_metrics", ...],
data:     "resources": ["metricflow/config", "metricflow/status"],
data:     "streaming": true
data:   }
data: }

event: heartbeat
data: {
data:   "timestamp": 1640995200.123,
data:   "connection_id": "conn_1234567890"
data: }
```

## API Examples

### Using MCP Tools

```python
from mcp_metricflow.server import list_metrics, query_metrics, QueryRequest

# List available metrics
metrics = list_metrics()
print(f"Found {len(metrics)} metrics")

# Execute a query
request = QueryRequest(
    metrics=["revenue"],
    dimensions=["metric_time"],
    where="metric_time >= '2023-01-01'",
    limit=100
)
results = query_metrics(request)
print(results)
```

### SSE Client Example

```python
import requests
import json

def listen_to_sse():
    response = requests.get(
        'http://localhost:8080/sse',
        headers={'Accept': 'text/event-stream'},
        stream=True
    )

    for line in response.iter_lines():
        if line:
            line = line.decode('utf-8')
            if line.startswith('data: '):
                data = line[6:]  # Remove 'data: ' prefix
                try:
                    event_data = json.loads(data)
                    print(f"Event: {event_data}")
                except json.JSONDecodeError:
                    print(f"Raw data: {data}")

listen_to_sse()
```

### JavaScript SSE Client

```javascript
const eventSource = new EventSource('http://localhost:8080/sse');

eventSource.onopen = function(event) {
    console.log('SSE connection opened');
};

eventSource.onmessage = function(event) {
    const data = JSON.parse(event.data);
    console.log('Received:', data);
};

eventSource.addEventListener('query_results', function(event) {
    const results = JSON.parse(event.data);
    console.log('Query results:', results);
});

eventSource.onerror = function(event) {
    console.error('SSE error:', event);
};
```

## Configuration

### Environment Variables

- **`MF_PROJECT_DIR`**: MetricFlow project directory (default: `~/.metricflow`)
- **`MF_VERBOSE`**: Enable verbose logging (default: `true`)
- **`MF_MODEL_PATH`**: Path to semantic models (default: `~/.metricflow/semantic_models`)
- **`MCP_HOST`**: MCP server host (default: `0.0.0.0`)
- **`MCP_PORT`**: MCP server port (default: `8080`)

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

### Health Check Endpoint

```bash
curl http://localhost:8080/sse/health
```

Response:
```json
{
  "status": "healthy",
  "active_connections": 2,
  "timestamp": 1640995200.123
}
```

### Docker Health Checks

```yaml
healthcheck:
  test: ["CMD", "curl", "-f", "http://localhost:8080/sse/health"]
  interval: 30s
  timeout: 10s
  retries: 3
```

## Troubleshooting

### Common Issues

1. **MCP server won't start**
   ```bash
   # Check MetricFlow setup
   mcp-metricflow test
   datus-mf status
   ```

2. **SSE connection fails**
   ```bash
   # Check if server is running
   curl http://localhost:8080/sse/health

   # Check logs
   docker logs mcp-metricflow-server
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

# Check connection status
curl -N http://localhost:8080/sse | head -20
```

## Development

### Project Structure

```
mcp_metricflow/
├── __init__.py          # Package initialization
├── server.py            # Main MCP server implementation
├── cli.py               # Command-line interface
└── sse_transport.py     # SSE transport implementation
```

### Adding New Tools

```python
@mcp.tool()
def my_custom_tool(param: str) -> Dict[str, Any]:
    """Custom tool description"""
    # Implementation
    return {"result": "success"}
```

### Adding New Resources

```python
@mcp.resource("my/resource")
async def my_resource() -> str:
    """Custom resource description"""
    return "Resource content"
```

## Integration with LLMs

The MCP server can be integrated with various LLM clients that support the Model Context Protocol:


- Claude Desktop
- Claude Code
- Custom MCP clients
- AI agents and assistants

Refer to the [Model Context Protocol documentation](https://modelcontextprotocol.io/) for client integration details.