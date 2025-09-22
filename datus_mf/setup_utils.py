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
            # Use mf tutorial generated database
            demo_db_path = self.mf_config_dir / "duck.db"
            config = {
                'model_path': str(self.mf_config_dir / "semantic_models"),
                'email': '',
                'dwh_schema': 'mf_demo',
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

    def setup_environment_variables(self):
        """Setup environment variables for the integration."""
        env_vars = {
            'MF_PROJECT_DIR': str(self.mf_config_dir),
            'MF_VERBOSE': 'true',
            'MF_MODEL_PATH': str(self.mf_config_dir / "semantic_models")
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

    def run_mf_tutorial(self):
        """Run mf tutorial to create demo database and semantic models."""
        try:
            # Set environment variables for mf tutorial
            env = os.environ.copy()
            env["MF_MODEL_PATH"] = str(self.mf_config_dir / "semantic_models")

            # Run mf tutorial command (without --skip-dw to allow schema creation in health checks)
            result = subprocess.run(
                ["mf", "tutorial"],
                cwd=str(self.mf_config_dir),
                env=env,
                input="y\n",  # Auto-confirm health checks
                text=True
                # No capture_output=True, so spinner can work normally
            )

            if result.returncode == 0:
                demo_db_path = self.mf_config_dir / "duck.db"
                if demo_db_path.exists():
                    click.echo(f"✅ Created tutorial database at {demo_db_path}")
                    click.echo("✅ Generated semantic models in semantic_models/")
                    return demo_db_path
                else:
                    click.echo("⚠️  Tutorial completed but duck.db not found")
                    return None
            else:
                click.echo(f"❌ mf tutorial failed: {result.stderr}")
                return None

        except FileNotFoundError:
            click.echo("❌ mf command not found. Please ensure MetricFlow is installed.")
            return None
        except Exception as e:
            click.echo(f"❌ Error running mf tutorial: {e}")
            return None
