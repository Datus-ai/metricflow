# Datus MetricFlow Integration

A simplified wrapper for MetricFlow that provides one-command setup for integration with Datus Agent.

## Quick Start

### Install with pipx (Recommended)

```bash
# Install with Python 3.9 (required for MetricFlow)
pipx install --python 3.9 .

# Or install from current directory
pipx install --python 3.9 /path/to/metricflow
```

### Setup Integration

#### Demo Setup (Recommended for testing)
```bash
# Setup with demo data and DuckDB (everything automated)
datus-metricflow setup --demo

# Source environment variables
source ~/.metricflow/datus_env.sh

# Test the setup
datus-metricflow validate
```

#### Custom Setup (For production use)
```bash
# Setup for your database (interactive)
datus-metricflow setup --dialect snowflake  # or bigquery, redshift, postgresql

# Edit configuration with your credentials
nano ~/.metricflow/config.yml

# Test connection
datus-metricflow health-checks

# Validate setup
datus-metricflow validate
```

## Commands

- `datus-metricflow setup [--demo] [--dialect DIALECT]` - Setup integration
- `datus-metricflow validate` - Validate MetricFlow configurations  
- `datus-metricflow health-checks` - Test database connection
- `datus-metricflow status` - Show integration status
- `datus-metricflow query -q "your question"` - Query helper (requires Datus Agent)

## What Gets Setup

- **MetricFlow configuration** (`~/.metricflow/config.yml`)
- **Datus Agent configuration** (`~/.datus/conf/agent.yml`) 
- **Environment variables** (`~/.metricflow/datus_env.sh`)
- **Semantic models directory** (`~/.metricflow/semantic_models/`)
- **Demo database** (if using `--demo` flag)
- **MCP filesystem server** (if npm available)

## Integration with Datus Agent

After setup, start Datus CLI:

```bash
# Make sure environment is loaded
source ~/.metricflow/datus_env.sh

# Start Datus Agent
datus-cli --namespace local_duckdb

# Ask questions (in Datus CLI)
Datus-sql> /which state has the highest total asset value of failure bank?

# Generate metrics
Datus-sql> !gen_metrics
```

## Troubleshooting

### Check Status
```bash
datus-metricflow status
```

### Manual Environment Setup
If automatic setup fails:
```bash
# Set required environment variables
export MF_PROJECT_DIR=~/.metricflow  
export MF_VERBOSE=true
export FILESYSTEM_MCP_DIRECTORY=~/.metricflow/semantic_models
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