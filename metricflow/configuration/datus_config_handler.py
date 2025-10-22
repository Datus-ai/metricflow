import os
import pathlib
import re
from typing import Any, Dict, Optional

import yaml

from metricflow.configuration.constants import (
    CONFIG_DWH_DB,
    CONFIG_DWH_DIALECT,
    CONFIG_DWH_HOST,
    CONFIG_DWH_PASSWORD,
    CONFIG_DWH_PORT,
    CONFIG_DWH_SCHEMA,
    CONFIG_DWH_USER,
    CONFIG_DWH_WAREHOUSE,
    CONFIG_DWH_ACCOUNT,
    CONFIG_DWH_PROJECT_ID,
    CONFIG_MODEL_PATH,
    CONFIG_EMAIL,
)
from metricflow.configuration.yaml_handler import YamlFileHandler


class DatusConfigHandler(YamlFileHandler):
    """Config handler that reads from Datus agent.yml configuration."""

    def __init__(self, namespace: str) -> None:
        self.namespace = namespace
        self.datus_config = self._load_datus_config()
        self.db_config = self._get_namespace_db_config()

        # Call parent with a dummy path since we don't use it
        super().__init__(yaml_file_path=self._get_dummy_config_path())

    def _get_dummy_config_path(self) -> str:
        """Return a dummy config path for parent class."""
        config_dir = pathlib.Path.home() / ".metricflow"
        config_dir.mkdir(parents=True, exist_ok=True)
        return str(config_dir / "datus_config_dummy.yml")

    def _load_datus_config(self) -> Dict[str, Any]:
        """Load Datus agent configuration file.

        Priority: ~/.datus/conf/agent.yml > ./conf/agent.yml
        """
        home_config = pathlib.Path.home() / ".datus" / "conf" / "agent.yml"
        local_config = pathlib.Path("conf/agent.yml")

        config_path = None
        if home_config.exists():
            config_path = home_config
        elif local_config.exists():
            config_path = local_config
        else:
            raise FileNotFoundError(
                "Datus config not found. Expected at ~/.datus/conf/agent.yml or ./conf/agent.yml"
            )

        with open(config_path, "r", encoding="utf-8") as f:
            config = yaml.safe_load(f) or {}

        return config

    def _get_namespace_db_config(self) -> Dict[str, Any]:
        """Get database configuration for the specified namespace."""
        namespaces = self.datus_config.get("agent", {}).get("namespace", {})

        if self.namespace not in namespaces:
            available = ", ".join(namespaces.keys())
            raise ValueError(
                f"Namespace '{self.namespace}' not found in Datus config. "
                f"Available namespaces: {available}"
            )

        return namespaces[self.namespace]

    def _resolve_env_vars(self, value: Any) -> str:
        """Resolve ${VAR} or $VAR style environment variables in config values."""
        if value is None:
            return ""

        value_str = str(value)

        if "$" not in value_str:
            return value_str

        # Replace ${VAR} style
        value_str = re.sub(
            r"\$\{([^}]+)\}",
            lambda m: os.environ.get(m.group(1), m.group(0)),
            value_str
        )

        # Replace $VAR style
        value_str = re.sub(
            r"\$([A-Za-z_][A-Za-z0-9_]*)",
            lambda m: os.environ.get(m.group(1), m.group(0)),
            value_str
        )

        return value_str

    def _get_model_path_from_datus_config(self) -> str:
        """Get semantic models path from Datus config.

        Reads from agent.storage.base_path and appends /semantic_models.
        Falls back to ~/.metricflow/semantic_models if not found.
        """
        storage_config = self.datus_config.get("agent", {}).get("storage", {})
        base_path = storage_config.get("base_path", "")

        if base_path:
            expanded_path = os.path.expanduser(base_path)
            expanded_path = os.path.expandvars(expanded_path)
            model_path = os.path.join(expanded_path, "semantic_models")
            return model_path

        # Default fallback
        return str(pathlib.Path.home() / ".metricflow" / "semantic_models")

    def get_value(self, key: str) -> Optional[str]:
        """Get configuration value by mapping Datus config to MetricFlow keys."""
        db_type = self.db_config.get("type", "").lower()

        # Handle model path specially
        if key == CONFIG_MODEL_PATH:
            return self._get_model_path_from_datus_config()

        # Handle email (optional)
        if key == CONFIG_EMAIL:
            return ""

        # Map database dialect
        if key == CONFIG_DWH_DIALECT:
            # Map Datus DB types to MetricFlow dialects
            dialect_mapping = {
                "postgres": "postgresql",
                "postgresql": "postgresql",
                "mysql": "mysql",
                "starrocks": "mysql",  # StarRocks uses MySQL protocol
                "clickhouse": "clickhouse",
                "duckdb": "duckdb",
                "sqlite": "sqlite",
                "snowflake": "snowflake",
                "bigquery": "bigquery",
            }
            return dialect_mapping.get(db_type, db_type)

        # Common mappings for most SQL databases
        if key == CONFIG_DWH_HOST:
            return self._resolve_env_vars(self.db_config.get("host", ""))

        if key == CONFIG_DWH_PORT:
            port = self.db_config.get("port")
            return self._resolve_env_vars(port) if port else ""

        if key == CONFIG_DWH_USER:
            return self._resolve_env_vars(self.db_config.get("username", ""))

        if key == CONFIG_DWH_PASSWORD:
            return self._resolve_env_vars(self.db_config.get("password", ""))

        if key == CONFIG_DWH_DB:
            # Special handling for file-based databases
            if db_type in ("sqlite", "duckdb"):
                uri = self._resolve_env_vars(self.db_config.get("uri", ""))
                # Remove protocol prefix if present
                if uri.startswith(f"{db_type}:///"):
                    uri = uri[len(f"{db_type}:///"):]
                return os.path.expanduser(uri)
            else:
                return self._resolve_env_vars(self.db_config.get("database", ""))

        if key == CONFIG_DWH_SCHEMA:
            schema = self.db_config.get("schema")
            if schema:
                return self._resolve_env_vars(schema)
            # Default schemas for different DB types
            if db_type == "duckdb":
                return "main"
            elif db_type in ("sqlite", "mysql"):
                return "default"
            elif db_type == "starrocks":
                # For StarRocks, use database as schema since it doesn't have schema concept
                return self._resolve_env_vars(self.db_config.get("database", ""))
            else:
                return self._resolve_env_vars(self.db_config.get("schema", ""))

        # Snowflake-specific
        if key == CONFIG_DWH_WAREHOUSE:
            return self._resolve_env_vars(self.db_config.get("warehouse", ""))

        if key == CONFIG_DWH_ACCOUNT:
            return self._resolve_env_vars(self.db_config.get("account", ""))

        # BigQuery-specific
        if key == CONFIG_DWH_PROJECT_ID:
            return self._resolve_env_vars(self.db_config.get("project_id", ""))

        # Unknown key
        return None

    @property
    def dir_path(self) -> str:
        """Return config directory path."""
        return str(pathlib.Path.home() / ".metricflow")

    @property
    def file_path(self) -> str:
        """Return dummy config file path."""
        return self._get_dummy_config_path()

    @property
    def log_file_path(self) -> str:
        """Return log file path."""
        log_dir = pathlib.Path.home() / ".metricflow" / "logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        return str(log_dir / "metricflow.log")
