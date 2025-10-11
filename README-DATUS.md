# Datus MetricFlow Integration

A simplified wrapper for MetricFlow that provides one-command setup for integration with Datus Agent.

## Quick Start

### Installation

```bash
# Install in development mode
pip install -e .
```

### Setup Integration

#### Demo Setup (Recommended for testing)
```bash
# Setup with demo data and DuckDB (everything automated)
datus-mf setup --demo
```

#### Setup from Datus Config (Recommended for Datus users)
```bash
# Setup using existing Datus namespace configuration
# This will automatically load database configs from Datus agent.yml
datus-mf setup --namespace your_namespace
```

## Commands

- `datus-mf setup [--demo | --namespace NAMESPACE]` - Setup integration
  - `--demo`: Quick demo setup with DuckDB
  - `--namespace`: Load config from Datus agent configuration
- `mcp-metricflow serve` - Start MetricFlow MCP server

## What Gets Setup

- **Semantic models directory** (`~/.metricflow/semantic_models/`)
- **Environment settings file** (`~/.datus/metricflow/env_settings.yml`) - Persistent storage of environment variables
- **Environment variables** (set automatically and saved for future use)
- **Demo database** (if using `--demo` flag)
- **MCP filesystem server** (if npm available)
- **MCP MetricFlow server** (Model Context Protocol with JSON-RPC support)

## Integration with Datus Agent

After setup, start Datus CLI:

```bash
# Start Datus Agent (environment variables already set during setup)
datus-cli --namespace local_duckdb

# Ask questions (in Datus CLI)
Datus> /which state has the highest total asset value of failure bank?

# Generate metrics
Datus> !gen_metrics
```

## MCP Server Integration

Start the MetricFlow MCP server for LLM integration:

```bash
# Start MetricFlow MCP server
mcp-metricflow serve --host 0.0.0.0 --port 8080

# Test MCP server
mcp-metricflow test

# Test MCP endpoint
curl -X POST http://localhost:8080/mcp \
  -H "Content-Type: application/json" \
  -d '{"jsonrpc": "2.0", "method": "initialize", "id": 1}'
```

For detailed MCP server documentation, see [MCP-SERVER.md](MCP-SERVER.md).

## Troubleshooting

### Install MCP Server Manually
```bash
npm install -g @modelcontextprotocol/server-filesystem
```

## Using Native MetricFlow Commands

After running `datus-mf setup`, a shell script is generated at `~/.datus/metricflow/datus_env.sh` with all environment variables.

To use native `mf` commands, source this script first:

```bash
# Source the environment variables
source ~/.datus/metricflow/datus_env.sh

# Now use native MetricFlow commands
mf validate-configs
mf health-checks
mf query --metrics transactions --dimensions metric_time
mf list-metrics
```