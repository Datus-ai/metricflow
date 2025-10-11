"""
Utilities for setting up Datus MetricFlow integration.
"""

import os
import pathlib
import subprocess
from typing import Optional, Dict, Any

import click
import yaml


class DatusMetricFlowSetup:
    """Handles the setup and configuration of Datus MetricFlow integration."""

    def __init__(self):
        self.home_dir = pathlib.Path.home()
        self.mf_config_dir = self.home_dir / ".metricflow"
        self.datus_config_dir = self.home_dir / ".datus"
        self.datus_mf_config_dir = self.datus_config_dir / "metricflow"
        self.datus_config: Optional[Dict[str, Any]] = None

    def load_datus_config(self, config_file: str = "") -> Optional[Dict[str, Any]]:
        """Load Datus agent configuration file.

        Priority: config_file > ~/.datus/conf/agent.yml > ./conf/agent.yml
        """
        if config_file:
            config_path = pathlib.Path(config_file).expanduser()
            if not config_path.exists():
                click.echo(f"❌ Config file not found: {config_path}")
                return None
        else:
            # Check ~/.datus/conf/agent.yml first
            home_config = self.home_dir / ".datus" / "conf" / "agent.yml"
            if home_config.exists():
                config_path = home_config
            # Then check ./conf/agent.yml
            elif pathlib.Path("conf/agent.yml").exists():
                config_path = pathlib.Path("conf/agent.yml")
            else:
                click.echo("⚠️  No Datus config file found. Use --demo for demo setup.")
                return None

        try:
            with open(config_path, "r", encoding="utf-8") as f:
                self.datus_config = yaml.safe_load(f) or {}
                click.echo(f"✅ Loaded Datus config from {config_path}")
                return self.datus_config
        except yaml.YAMLError as e:
            click.echo(f"❌ Error parsing config file: {e}")
            return None
        except Exception as e:
            click.echo(f"❌ Error loading config file: {e}")
            return None

    def get_namespace_db_config(self, namespace: str) -> Optional[Dict[str, Any]]:
        """Get database configuration for a specific namespace."""
        if not self.datus_config:
            return None

        namespaces = self.datus_config.get("agent", {}).get("namespace", {})
        if namespace not in namespaces:
            click.echo(f"❌ Namespace '{namespace}' not found in config")
            click.echo(f"   Available namespaces: {', '.join(namespaces.keys())}")
            return None

        return namespaces[namespace]

    def _get_model_path_from_config(self) -> str:
        """Get semantic models path from Datus config or use default.

        Reads from agent.storage.base_path and appends /semantic_models.
        Falls back to ~/.metricflow/semantic_models if not found.
        """
        if self.datus_config:
            storage_config = self.datus_config.get("agent", {}).get("storage", {})
            base_path = storage_config.get("base_path", "")
            if base_path:
                # Expand ~ and environment variables in path
                expanded_path = os.path.expanduser(base_path)
                expanded_path = os.path.expandvars(expanded_path)
                # Append /semantic_models
                model_path = os.path.join(expanded_path, "semantic_models")
                return model_path

        # Default fallback
        return str(self.mf_config_dir / "semantic_models")

    def ensure_directories(self):
        """Create necessary directories."""
        self.mf_config_dir.mkdir(exist_ok=True)
        self.datus_config_dir.mkdir(parents=True, exist_ok=True)
        self.datus_mf_config_dir.mkdir(parents=True, exist_ok=True)
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

    def setup_environment_variables(self, namespace: Optional[str] = None, db_config: Optional[Dict[str, Any]] = None):
        """Setup environment variables for the integration.

        Args:
            namespace: Datus namespace name
            db_config: Database configuration from Datus config
        """
        # Determine MF_MODEL_PATH from Datus config or use default
        model_path = self._get_model_path_from_config()

        # Only set environment variables that MetricFlow actually uses
        env_vars = {
            "MF_MODEL_PATH": model_path,
        }

        # Add MF_PATH if mf command is available
        try:
            import shutil

            mf_path = shutil.which("mf")
            if mf_path:
                env_vars["MF_PATH"] = mf_path
        except Exception:
            pass

        # Add database-specific environment variables if db_config provided
        if db_config:
            db_type = db_config.get("type", "").lower()
            env_vars["MF_DWH_DIALECT"] = db_type

            # Resolve environment variables in config values
            def resolve_env(value) -> str:
                """Resolve ${VAR} or $VAR style environment variables."""
                # Convert to string first
                value_str = str(value) if value is not None else ""
                if "$" in value_str:
                    import re

                    # Replace ${VAR} style
                    value_str = re.sub(r"\$\{([^}]+)\}", lambda m: os.environ.get(m.group(1), m.group(0)), value_str)
                    # Replace $VAR style
                    value_str = re.sub(
                        r"\$([A-Za-z_][A-Za-z0-9_]*)", lambda m: os.environ.get(m.group(1), m.group(0)), value_str
                    )
                return value_str

            if db_type == "snowflake":
                env_vars["MF_DWH_HOST"] = resolve_env(db_config.get("host", ""))
                env_vars["MF_DWH_USER"] = resolve_env(db_config.get("username", ""))
                env_vars["MF_DWH_PASSWORD"] = resolve_env(db_config.get("password", ""))
                env_vars["MF_DWH_DB"] = resolve_env(db_config.get("database", ""))
                env_vars["MF_DWH_SCHEMA"] = resolve_env(db_config.get("schema", "default"))
                env_vars["MF_DWH_WAREHOUSE"] = resolve_env(db_config.get("warehouse", ""))
                if db_config.get("account"):
                    env_vars["MF_DWH_ACCOUNT"] = resolve_env(db_config.get("account", ""))
            elif db_type == "sqlite":
                uri = resolve_env(db_config.get("uri", ""))
                # SQLite URI can be file path or sqlite:/// format
                if uri.startswith("sqlite:///"):
                    db_path = uri[len("sqlite:///") :]
                else:
                    db_path = uri
                env_vars["MF_DWH_DB"] = os.path.expanduser(db_path)
                env_vars["MF_DWH_SCHEMA"] = "default"  # placeholder
            elif db_type == "duckdb":
                uri = resolve_env(db_config.get("uri", ""))
                if uri.startswith("duckdb:///"):
                    db_path = uri[len("duckdb:///") :]
                else:
                    db_path = uri
                env_vars["MF_DWH_DB"] = os.path.expanduser(db_path)
                env_vars["MF_DWH_SCHEMA"] = resolve_env(db_config.get("schema", "main"))
            elif db_type == "bigquery":
                env_vars["MF_DWH_PROJECT_ID"] = resolve_env(db_config.get("project_id", ""))
                env_vars["MF_DWH_DB"] = resolve_env(db_config.get("database", ""))
                env_vars["MF_DWH_SCHEMA"] = resolve_env(db_config.get("schema", "default"))
            elif db_type in ("postgres", "postgresql", "mysql", "starrocks", "clickhouse"):
                # Generic SQL database configuration
                # Map database types to MetricFlow dialects
                dialect_mapping = {
                    "postgres": "postgresql",
                    "postgresql": "postgresql",
                    "mysql": "mysql",
                    "starrocks": "mysql",  # StarRocks uses MySQL protocol
                    "clickhouse": "clickhouse",
                }
                env_vars["MF_DWH_DIALECT"] = dialect_mapping.get(db_type, db_type)
                env_vars["MF_DWH_HOST"] = resolve_env(db_config.get("host", ""))
                port_value = db_config.get("port", "")
                env_vars["MF_DWH_PORT"] = resolve_env(port_value) if port_value else ""
                env_vars["MF_DWH_USER"] = resolve_env(db_config.get("username", ""))
                env_vars["MF_DWH_PASSWORD"] = resolve_env(db_config.get("password", ""))
                env_vars["MF_DWH_DB"] = resolve_env(db_config.get("database", ""))
                env_vars["MF_DWH_SCHEMA"] = resolve_env(db_config.get("schema", "default"))

        # Set environment variables in current process
        for key, value in env_vars.items():
            if value:  # Only set non-empty values
                os.environ[key] = value

        # Save environment variables to file
        self._save_env_settings(env_vars, namespace)

        # Generate shell script for sourcing
        self._generate_env_script(env_vars, namespace)

        click.echo(f"✅ Environment variables configured")
        if namespace:
            click.echo(f"   Namespace: {namespace}")
        click.echo(f"   Database: {env_vars.get('MF_DWH_DIALECT', 'not set')}")
        click.echo(f"   Saved to: {self.datus_mf_config_dir / 'env_settings.yml'}")
        click.echo(f"   Shell script: {self.datus_mf_config_dir / 'datus_env.sh'}")
        click.echo(f"\n💡 To use with native mf commands, run:")
        click.echo(f"   source {self.datus_mf_config_dir / 'datus_env.sh'}")

        return env_vars

    def _save_env_settings(self, env_vars: Dict[str, str], namespace: Optional[str] = None):
        """Save environment variables to a YAML file for persistence.

        Args:
            env_vars: Dictionary of environment variables
            namespace: Optional namespace name for metadata
        """
        env_settings_path = self.datus_mf_config_dir / "env_settings.yml"

        settings = {
            "metadata": {
                "created_at": self._get_current_timestamp(),
                "namespace": namespace or "manual",
            },
            "environment_variables": env_vars,
        }

        with open(env_settings_path, "w") as f:
            yaml.dump(settings, f, default_flow_style=False, sort_keys=False)

    def load_env_settings(self) -> Optional[Dict[str, str]]:
        """Load environment variables from saved settings file.

        Returns:
            Dictionary of environment variables, or None if file doesn't exist
        """
        env_settings_path = self.datus_mf_config_dir / "env_settings.yml"

        if not env_settings_path.exists():
            return None

        try:
            with open(env_settings_path, "r") as f:
                settings = yaml.safe_load(f) or {}
                return settings.get("environment_variables", {})
        except Exception as e:
            click.echo(f"⚠️  Failed to load env settings: {e}")
            return None

    def apply_env_settings(self) -> bool:
        """Load and apply saved environment variables to current process.

        Returns:
            True if settings were loaded and applied, False otherwise
        """
        env_vars = self.load_env_settings()
        if not env_vars:
            return False

        for key, value in env_vars.items():
            if value:
                os.environ[key] = value

        return True

    def _generate_env_script(self, env_vars: Dict[str, str], namespace: Optional[str] = None):
        """Generate a shell script for sourcing environment variables.

        Args:
            env_vars: Dictionary of environment variables
            namespace: Optional namespace name for comments
        """
        env_script_path = self.datus_mf_config_dir / "datus_env.sh"

        script_lines = [
            "#!/bin/bash",
            "# Datus MetricFlow Environment Variables",
            f"# Generated at: {self._get_current_timestamp()}",
        ]

        if namespace:
            script_lines.append(f"# Namespace: {namespace}")

        script_lines.append("")

        for key, value in env_vars.items():
            if value:
                # Escape single quotes in value and use single quotes for shell safety
                escaped_value = value.replace("'", "'\\''")
                script_lines.append(f"export {key}='{escaped_value}'")

        script_content = "\n".join(script_lines) + "\n"

        with open(env_script_path, "w") as f:
            f.write(script_content)

        # Make script executable
        os.chmod(env_script_path, 0o755)

    @staticmethod
    def _get_current_timestamp() -> str:
        """Get current timestamp in ISO format."""
        from datetime import datetime

        return datetime.now().isoformat()

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
