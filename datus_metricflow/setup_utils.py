"""
Utilities for setting up Datus MetricFlow integration.
"""

import os
import pathlib
import shutil
import subprocess
import sys
from typing import Optional

import click
import yaml


class DatusMetricFlowSetup:
    """Handles the setup and configuration of Datus MetricFlow integration."""
    
    def __init__(self):
        self.home_dir = pathlib.Path.home()
        self.mf_config_dir = self.home_dir / ".metricflow"
        self.datus_config_dir = self.home_dir / ".datus"
        
    def ensure_directories(self):
        """Create necessary directories."""
        self.mf_config_dir.mkdir(exist_ok=True)
        self.datus_config_dir.mkdir(parents=True, exist_ok=True)
        (self.datus_config_dir / "conf").mkdir(exist_ok=True)
        (self.datus_config_dir / "demo").mkdir(exist_ok=True)
        
    def create_metricflow_config(self, dialect: str = "duckdb"):
        """Create MetricFlow configuration file."""
        config_path = self.mf_config_dir / "config.yml"
        
        if dialect == "duckdb":
            demo_db_path = self.datus_config_dir / "demo" / "demo.duckdb"
            config = {
                'model_path': str(self.mf_config_dir / "semantic_models"),
                'email': '',
                'dwh_schema': 'demo',
                'dwh_dialect': 'duckdb',
                'dwh_database': str(demo_db_path)
            }
        else:
            # For other databases, create template
            config = {
                'model_path': str(self.mf_config_dir / "semantic_models"),
                'email': '',
                'dwh_schema': 'your_schema',
                'dwh_dialect': dialect,
                'dwh_database': 'your_database'
            }
            
        with open(config_path, 'w') as f:
            yaml.dump(config, f, default_flow_style=False)
            
        click.echo(f"✅ Created MetricFlow config at {config_path}")
        return config_path
        
    def create_datus_agent_config(self):
        """Create Datus Agent configuration."""
        agent_config_path = self.datus_config_dir / "conf" / "agent.yml"
        
        # Check if config already exists
        if agent_config_path.exists():
            with open(agent_config_path, 'r') as f:
                config = yaml.safe_load(f) or {}
        else:
            config = {}
            
        # Add metrics section if not exists
        if 'metrics' not in config:
            config['metrics'] = {
                'demo': {
                    'domain': 'economic',
                    'layer1': 'bank',
                    'layer2': 'bank_failures'
                }
            }
            
            with open(agent_config_path, 'w') as f:
                yaml.dump(config, f, default_flow_style=False)
                
            click.echo(f"✅ Created Datus Agent config at {agent_config_path}")
        else:
            click.echo(f"📝 Datus Agent config already exists at {agent_config_path}")
            
        return agent_config_path
        
    def setup_environment_variables(self):
        """Setup environment variables for the integration."""
        env_vars = {
            'MF_PROJECT_DIR': str(self.mf_config_dir),
            'MF_VERBOSE': 'true',
            'FILESYSTEM_MCP_DIRECTORY': str(self.mf_config_dir / "semantic_models")
        }
        
        # Create a shell script for environment setup
        env_script_path = self.mf_config_dir / "datus_env.sh"
        with open(env_script_path, 'w') as f:
            f.write("#!/bin/bash\n")
            f.write("# Datus MetricFlow Environment Variables\n")
            for key, value in env_vars.items():
                f.write(f'export {key}="{value}"\n')
                
        env_script_path.chmod(0o755)
        
        click.echo(f"✅ Created environment script at {env_script_path}")
        click.echo("💡 To use: source ~/.metricflow/datus_env.sh")
        
        return env_vars
        
    def check_npm_and_install_filesystem_server(self):
        """Check for npm and install filesystem MCP server."""
        try:
            # Check if npm is available
            subprocess.run(['npm', '--version'], check=True, capture_output=True)
            
            # Install filesystem MCP server
            subprocess.run([
                'npm', 'install', '-g', '@modelcontextprotocol/server-filesystem'
            ], check=True)
            
            click.echo("✅ Installed filesystem MCP server")
            return True
            
        except (subprocess.CalledProcessError, FileNotFoundError):
            click.echo("⚠️  npm not found. Please install Node.js and npm manually.")
            click.echo("   Then run: npm install -g @modelcontextprotocol/server-filesystem")
            return False
            
    def create_demo_database(self):
        """Create demo database with sample data."""
        try:
            import duckdb
            
            demo_db_path = self.datus_config_dir / "demo" / "demo.duckdb"
            
            # Create demo database with sample bank failures data
            conn = duckdb.connect(str(demo_db_path))
            
            # Create schema and sample table
            conn.execute("CREATE SCHEMA IF NOT EXISTS demo")
            conn.execute("CREATE SCHEMA IF NOT EXISTS main")
            
            # Sample bank failures data
            conn.execute("""
                CREATE OR REPLACE TABLE demo.main.bank_failures AS
                SELECT * FROM VALUES
                    ('CA', 'Bank of California', 1000.5, '2023-01-15'),
                    ('NY', 'Empire Bank', 2500.8, '2023-02-20'),
                    ('TX', 'Lone Star Bank', 1800.2, '2023-03-10'),
                    ('FL', 'Sunshine Bank', 3200.7, '2023-04-05'),
                    ('CA', 'Golden Gate Bank', 1200.3, '2023-05-12')
                AS t(State, "Bank Name", "Assets ($mil.)", "Closing Date")
            """)
            
            conn.close()
            click.echo(f"✅ Created demo database at {demo_db_path}")
            return demo_db_path
            
        except ImportError:
            click.echo("⚠️  DuckDB not available. Demo database not created.")
            return None