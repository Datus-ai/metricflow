import os
import pathlib
from typing import Dict, Optional

from metricflow.configuration.constants import (
    CONFIG_DWH_ACCOUNT,
    CONFIG_DWH_DB,
    CONFIG_DWH_DIALECT,
    CONFIG_DWH_HOST,
    CONFIG_DWH_PASSWORD,
    CONFIG_DWH_PORT,
    CONFIG_DWH_PROJECT_ID,
    CONFIG_DWH_SCHEMA,
    CONFIG_DWH_USER,
    CONFIG_DWH_WAREHOUSE,
    CONFIG_EMAIL,
    CONFIG_MODEL_PATH,
)
from metricflow.configuration.yaml_handler import YamlFileHandler


# Mapping from Datus DB types to MetricFlow dialect names
DIALECT_MAPPING = {
    "postgres": "postgresql",
    "postgresql": "postgresql",
    "greenplum": "greenplum",
    "mysql": "mysql",
    "starrocks": "starrocks",
    "clickhouse": "clickhouse",
    "trino": "trino",
    "duckdb": "duckdb",
    "sqlite": "sqlite",
    "snowflake": "snowflake",
    "bigquery": "bigquery",
}

# Default schema values per DB type
DEFAULT_SCHEMA_MAPPING = {
    "duckdb": "main",
    "sqlite": "default",
    "mysql": "default",
    "postgres": "public",
    "postgresql": "public",
    "greenplum": "public",
}


def build_config_dict_from_db_params(
    db_type: str,
    host: str = "",
    port: str = "",
    username: str = "",
    password: str = "",
    database: str = "",
    schema: str = "",
    uri: str = "",
    warehouse: str = "",
    account: str = "",
    project_id: str = "",
    model_path: str = "",
) -> Dict[str, str]:
    """Build a MetricFlow config dict from database connection parameters.

    This translates Datus-style DB params into the CONFIG_* key/value pairs
    that MetricFlow expects, so callers don't need to know MetricFlow internals.

    Values should be pre-resolved (environment variables already expanded).
    """
    db_type_lower = db_type.lower()
    result: Dict[str, str] = {}

    # Dialect
    result[CONFIG_DWH_DIALECT] = DIALECT_MAPPING.get(db_type_lower, db_type_lower)

    # Host / Port / User / Password
    result[CONFIG_DWH_HOST] = host
    result[CONFIG_DWH_PORT] = port
    result[CONFIG_DWH_USER] = username
    result[CONFIG_DWH_PASSWORD] = password

    # Database — file-based DBs use uri
    if db_type_lower in ("sqlite", "duckdb") and uri:
        db_path = uri
        prefix = f"{db_type_lower}:///"
        if db_path.startswith(prefix):
            db_path = db_path[len(prefix):]
        result[CONFIG_DWH_DB] = os.path.expanduser(db_path)
    else:
        result[CONFIG_DWH_DB] = database

    # Schema — with defaults per DB type
    if schema:
        result[CONFIG_DWH_SCHEMA] = schema
    elif db_type_lower == "starrocks":
        result[CONFIG_DWH_SCHEMA] = database
    elif db_type_lower == "clickhouse":
        result[CONFIG_DWH_SCHEMA] = database
    elif db_type_lower == "trino":
        result[CONFIG_DWH_SCHEMA] = "default"
    elif db_type_lower in DEFAULT_SCHEMA_MAPPING:
        result[CONFIG_DWH_SCHEMA] = DEFAULT_SCHEMA_MAPPING[db_type_lower]
    else:
        result[CONFIG_DWH_SCHEMA] = ""

    # Snowflake-specific
    result[CONFIG_DWH_WAREHOUSE] = warehouse
    result[CONFIG_DWH_ACCOUNT] = account

    # BigQuery-specific
    result[CONFIG_DWH_PROJECT_ID] = project_id

    # Email (not used in dict-config mode, but keep parity with DatusConfigHandler)
    result[CONFIG_EMAIL] = ""

    # Model path
    if model_path:
        result[CONFIG_MODEL_PATH] = model_path

    return result


class DictConfigHandler(YamlFileHandler):
    """Config handler that reads from a pre-built dict instead of a file.

    This allows callers (e.g. MetricFlowAdapter) to pass in already-parsed
    configuration without requiring MetricFlow to read agent.yml again.
    """

    def __init__(self, config_dict: Dict[str, str]) -> None:
        self._config_dict = config_dict
        super().__init__(yaml_file_path=self._get_dummy_config_path())

    @staticmethod
    def _get_dummy_config_path() -> str:
        config_dir = pathlib.Path.home() / ".metricflow"
        config_dir.mkdir(parents=True, exist_ok=True)
        return str(config_dir / "dict_config_dummy.yml")

    def get_value(self, key: str) -> Optional[str]:
        """Get configuration value from the in-memory dict."""
        return self._config_dict.get(key)

    @property
    def dir_path(self) -> str:
        return str(pathlib.Path.home() / ".metricflow")

    @property
    def file_path(self) -> str:
        return self._get_dummy_config_path()

    @property
    def url(self) -> str:
        """Return a descriptive url indicating this is an in-memory config."""
        return "dict-config://in-memory"

    @property
    def log_file_path(self) -> str:
        log_dir = pathlib.Path.home() / ".metricflow" / "logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        return str(log_dir / "metricflow.log")
