# Datus MetricFlow Integration

MetricFlow with native Datus config integration - no environment variables needed!

## Quick Start

### Installation

```bash
# Install in development mode
pip install -e .
```

### Setup Integration

#### Option 1: Demo Setup (Recommended for testing)
```bash
# Setup with demo data and DuckDB
mf tutorial
```

#### Option 2: Setup from Datus Config (Recommended for Datus users)
```bash
# Validate your Datus namespace configuration
mf setup --namespace your_namespace
```

#### Option 3: Traditional Setup
```bash
# Interactive setup with config file
mf setup
```

## Using MetricFlow with Datus

### With Namespace (reads from Datus agent.yml)
```bash
# All commands support --namespace flag
mf --namespace starrocks list-metrics
mf --namespace starrocks query --metrics revenue --dimensions metric_time
mf --namespace starrocks health-checks
```

### Traditional Mode (reads from ~/.metricflow/config.yml)
```bash
# Use without --namespace flag
mf list-metrics
mf query --metrics revenue --dimensions metric_time
mf health-checks
```

## Commands

- `mf setup [--namespace NAMESPACE]` - Setup and validate configuration
- `mf tutorial` - Create demo database and sample models
- `mf --namespace <NAME> <command>` - Use specific Datus namespace
- `mcp-metricflow serve` - Start MetricFlow MCP server

## Configuration

### Namespace Mode (No config file needed!)
When using `--namespace`, MetricFlow reads directly from `~/.datus/conf/agent.yml`:

```yaml
agent:
  namespace:
    starrocks:
      type: mysql
      host: localhost
      port: 9030
      username: admin
      password: ${DB_PASSWORD}
      database: my_db
      schema: analytics
```

### Traditional Mode
Config file at `~/.metricflow/config.yml`:

```yaml
dwh_dialect: duckdb
dwh_database: /path/to/duck.db
dwh_schema: main
model_path: ~/.metricflow/semantic_models
```

## Integration with Datus Agent

```bash
# Start Datus Agent with namespace
datus-cli --namespace starrocks

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

## Architecture

### Dual Configuration Mode

```
┌─────────────────────────────────────────────────────────┐
│                    mf CLI                                │
├─────────────────────────────────────────────────────────┤
│  With --namespace    │  Without --namespace              │
├──────────────────────┼───────────────────────────────────┤
│ DatusConfigHandler   │  ConfigHandler                    │
│       ↓              │       ↓                            │
│ ~/.datus/conf/       │  ~/.metricflow/                   │
│   agent.yml          │    config.yml                     │
│       ↓              │       ↓                            │
│ Direct mapping       │  Direct YAML read                 │
└──────────────────────┴───────────────────────────────────┘
```

### Key Features

- ✅ **No environment variables** - Direct config file reading
- ✅ **Dual mode support** - Namespace or traditional config
- ✅ **Single command** - Just `mf` with optional `--namespace`
- ✅ **Lazy initialization** - Config loaded on-demand
- ✅ **Environment variable resolution** - Supports `${VAR}` in Datus config

## Supported Databases

| Database | Status | Notes |
|----------|--------|-------|
| DuckDB | ✅ Full | File-based, perfect for demos |
| SQLite | ✅ Full | File-based |
| MySQL | ✅ Full | Network database |
| StarRocks | ✅ Full | Uses MySQL protocol |
| PostgreSQL | ⚠️ Config only | Client not in this build |
| Snowflake | ⚠️ Config only | Client not in this build |
| BigQuery | ⚠️ Config only | Client not in this build |