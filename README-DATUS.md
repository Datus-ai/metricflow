# Datus MetricFlow Integration

A simplified wrapper for MetricFlow that provides one-command setup for integration with Datus Agent.

## Quick Start

### Install with pipx (Recommended)

```bash
# Install with Python 3.12 (required for MetricFlow)
pipx install --python 3.12 .

# Or install from current directory
pipx install --python 3.12 /path/to/metricflow
```

### Setup Integration

#### Demo Setup (Recommended for testing)
```bash
# Setup with demo data and DuckDB (everything automated)
datus-mf setup --demo

# Source environment variables
source ~/.metricflow/datus_env.sh

# Test the setup
datus-mf validate
```

#### Custom Setup (For production use)
```bash
# Setup for your database (interactive)
datus-mf setup --dialect snowflake  # or bigquery, redshift, postgresql

# Edit configuration with your credentials
nano ~/.metricflow/config.yml

# Test connection
datus-mf health-checks

# Validate setup
datus-mf validate
```

## Commands

- `datus-mf setup [--demo] [--dialect DIALECT]` - Setup integration
- `datus-mf validate` - Validate MetricFlow configurations
- `datus-mf health-checks` - Test database connection
- `datus-mf status` - Show integration status
- `datus-mf query -q "your question"` - Query helper (requires Datus Agent)
- `mcp-metricflow serve` - Start MCP server with SSE support

## What Gets Setup

- **MetricFlow configuration** (`~/.metricflow/config.yml`)
- **Environment variables** (`~/.metricflow/datus_env.sh`)
- **Semantic models directory** (`~/.metricflow/semantic_models/`)
- **Demo database** (if using `--demo` flag)
- **MCP filesystem server** (if npm available)
- **MCP MetricFlow server** (Model Context Protocol with SSE support)

## Integration with Datus Agent

After setup, start Datus CLI:

```bash
# Make sure environment is loaded
source ~/.metricflow/datus_env.sh

# Start Datus Agent
datus-cli --namespace local_duckdb

# Ask questions (in Datus CLI)
Datus> /which state has the highest total asset value of failure bank?

# Generate metrics
Datus> !gen_metrics
```

## MCP Server Integration

Start the MetricFlow MCP server for LLM integration:

```bash
# Start MCP server with SSE support
mcp-metricflow serve --host 0.0.0.0 --port 8080

# Test MCP server
mcp-metricflow test

# Access SSE endpoint
curl -N http://localhost:8080/sse
```

For detailed MCP server documentation, see [MCP-SERVER.md](MCP-SERVER.md).

## Troubleshooting

### Check Status
```bash
datus-mf status
```

### Manual Environment Setup
If automatic setup fails:
```bash
# Set required environment variables
export MF_PROJECT_DIR=~/.metricflow
export MF_VERBOSE=true
export MF_MODEL_PATH=~/.metricflow/semantic_models
```

### Install MCP Server Manually
```bash
npm install -g @modelcontextprotocol/server-filesystem
```

## Original MetricFlow Commands

All original MetricFlow commands are still available via the `mf` command:

```bash
mf validate-configs
mf health-checks
mf query --metrics transactions --dimensions metric_time
mf list-metrics
```