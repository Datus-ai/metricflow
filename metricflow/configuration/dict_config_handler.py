import os
import pathlib
from typing import Any, Dict, Mapping, Optional

from metricflow.configuration.constants import (
    CONFIG_DWH_ACCOUNT,
    CONFIG_DWH_DB,
    CONFIG_DWH_DIALECT,
    CONFIG_DWH_HOST,
    CONFIG_DWH_PASSWORD,
    CONFIG_DWH_PORT,
    CONFIG_DWH_PRIVATE_KEY,
    CONFIG_DWH_PRIVATE_KEY_FILE,
    CONFIG_DWH_PRIVATE_KEY_FILE_PWD,
    CONFIG_DWH_PROJECT_ID,
    CONFIG_DWH_ROLE,
    CONFIG_DWH_SCHEMA,
    CONFIG_DWH_SSLMODE,
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

SCHEMA_EQUALS_DATABASE_TYPES = {"mysql", "starrocks", "clickhouse"}


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
    sslmode: str = "",
    role: str = "",
    private_key: str = "",
    private_key_file: str = "",
    private_key_file_pwd: str = "",
    catalog: str = "",
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
    result[CONFIG_DWH_SSLMODE] = sslmode

    # Database — file-based DBs use uri; Trino uses catalog as the URL database segment.
    if db_type_lower in ("sqlite", "duckdb") and uri:
        db_path = uri
        prefix = f"{db_type_lower}:///"
        if db_path.startswith(prefix):
            db_path = db_path[len(prefix) :]
        result[CONFIG_DWH_DB] = os.path.expanduser(db_path)
    elif db_type_lower == "trino" and catalog:
        result[CONFIG_DWH_DB] = catalog
    else:
        result[CONFIG_DWH_DB] = database

    # Schema — with defaults per DB type
    if schema:
        result[CONFIG_DWH_SCHEMA] = schema
    elif db_type_lower == "trino" and catalog and database:
        result[CONFIG_DWH_SCHEMA] = database
    elif db_type_lower in SCHEMA_EQUALS_DATABASE_TYPES and database:
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
    result[CONFIG_DWH_ROLE] = role
    result[CONFIG_DWH_PRIVATE_KEY] = private_key
    result[CONFIG_DWH_PRIVATE_KEY_FILE] = private_key_file
    result[CONFIG_DWH_PRIVATE_KEY_FILE_PWD] = private_key_file_pwd

    # BigQuery-specific
    result[CONFIG_DWH_PROJECT_ID] = project_id

    # Email (not used in dict-config mode, but keep parity with DatusConfigHandler)
    result[CONFIG_EMAIL] = ""

    # Model path
    if model_path:
        result[CONFIG_MODEL_PATH] = model_path

    return result


def _string_config_value(value: Any) -> str:
    if value is None:
        return ""
    return str(value)


def build_config_dict_from_datus_datasource(db_config: Mapping[str, Any], model_path: str = "") -> Dict[str, str]:
    """Build a MetricFlow config dict from a raw Datus datasource config."""
    database = db_config.get("database")
    if database is None or database == "":
        database = db_config.get("database_name", "")

    schema = db_config.get("schema")
    if schema is None or schema == "":
        schema = db_config.get("db_schema") or db_config.get("schema_name", "")

    catalog = db_config.get("catalog")
    if catalog is None or catalog == "":
        catalog = db_config.get("catalog_name", "")

    return build_config_dict_from_db_params(
        db_type=_string_config_value(db_config.get("type")),
        host=_string_config_value(db_config.get("host")),
        port=_string_config_value(db_config.get("port")),
        username=_string_config_value(db_config.get("username")),
        password=_string_config_value(db_config.get("password")),
        database=_string_config_value(database),
        schema=_string_config_value(schema),
        uri=_string_config_value(db_config.get("uri")),
        warehouse=_string_config_value(db_config.get("warehouse")),
        account=_string_config_value(db_config.get("account")),
        project_id=_string_config_value(db_config.get("project_id")),
        model_path=model_path,
        sslmode=_string_config_value(db_config.get("sslmode")),
        role=_string_config_value(db_config.get("role")),
        private_key=_string_config_value(db_config.get("private_key")),
        private_key_file=_string_config_value(db_config.get("private_key_file")),
        private_key_file_pwd=_string_config_value(db_config.get("private_key_file_pwd")),
        catalog=_string_config_value(catalog),
    )


class DictConfigHandler(YamlFileHandler):
    """Config handler that reads from a pre-built dict instead of a file.

    This allows callers (e.g. MetricFlowAdapter) to pass in already-parsed
    configuration without requiring MetricFlow to read agent.yml again.
    """

    def __init__(self, config_dict: Dict[str, str]) -> None:
        """Initialize DictConfigHandler with a pre-built config dict."""
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
