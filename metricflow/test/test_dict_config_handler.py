import pathlib

import pytest

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
    CONFIG_MODEL_PATH,
)
from metricflow.configuration.dict_config_handler import (
    DictConfigHandler,
    build_config_dict_from_db_params,
)


class TestBuildConfigDictFromDbParams:
    """Tests for build_config_dict_from_db_params()."""

    def test_mysql(self):
        result = build_config_dict_from_db_params(
            db_type="mysql",
            host="localhost",
            port="3306",
            username="root",
            password="secret",
            database="mydb",
        )
        assert result[CONFIG_DWH_DIALECT] == "mysql"
        assert result[CONFIG_DWH_HOST] == "localhost"
        assert result[CONFIG_DWH_PORT] == "3306"
        assert result[CONFIG_DWH_USER] == "root"
        assert result[CONFIG_DWH_PASSWORD] == "secret"
        assert result[CONFIG_DWH_DB] == "mydb"
        assert result[CONFIG_DWH_SCHEMA] == "default"

    def test_starrocks(self):
        result = build_config_dict_from_db_params(
            db_type="starrocks",
            host="sr-host",
            port="9030",
            username="admin",
            password="pw",
            database="sr_db",
        )
        assert result[CONFIG_DWH_DIALECT] == "mysql"
        assert result[CONFIG_DWH_SCHEMA] == "sr_db"

    def test_postgresql(self):
        result = build_config_dict_from_db_params(
            db_type="postgresql",
            host="pg-host",
            port="5432",
            username="pguser",
            password="pgpw",
            database="pgdb",
        )
        assert result[CONFIG_DWH_DIALECT] == "postgresql"
        assert result[CONFIG_DWH_SCHEMA] == "public"

    def test_postgres_alias(self):
        result = build_config_dict_from_db_params(db_type="postgres")
        assert result[CONFIG_DWH_DIALECT] == "postgresql"
        assert result[CONFIG_DWH_SCHEMA] == "public"

    def test_duckdb_with_uri(self):
        result = build_config_dict_from_db_params(
            db_type="duckdb",
            uri="duckdb:///~/data/my.db",
        )
        assert result[CONFIG_DWH_DIALECT] == "duckdb"
        expected_path = str(pathlib.Path.home() / "data/my.db")
        assert result[CONFIG_DWH_DB] == expected_path
        assert result[CONFIG_DWH_SCHEMA] == "main"

    def test_duckdb_without_prefix(self):
        result = build_config_dict_from_db_params(
            db_type="duckdb",
            uri="~/data/my.db",
        )
        expected_path = str(pathlib.Path.home() / "data/my.db")
        assert result[CONFIG_DWH_DB] == expected_path

    def test_sqlite(self):
        result = build_config_dict_from_db_params(
            db_type="sqlite",
            uri="sqlite:///~/test.db",
        )
        assert result[CONFIG_DWH_DIALECT] == "sqlite"
        expected_path = str(pathlib.Path.home() / "test.db")
        assert result[CONFIG_DWH_DB] == expected_path
        assert result[CONFIG_DWH_SCHEMA] == "default"

    def test_snowflake(self):
        result = build_config_dict_from_db_params(
            db_type="snowflake",
            host="account.snowflakecomputing.com",
            username="sf_user",
            password="sf_pw",
            database="sf_db",
            schema="sf_schema",
            warehouse="wh1",
            account="my_account",
        )
        assert result[CONFIG_DWH_DIALECT] == "snowflake"
        assert result[CONFIG_DWH_WAREHOUSE] == "wh1"
        assert result[CONFIG_DWH_ACCOUNT] == "my_account"
        assert result[CONFIG_DWH_SCHEMA] == "sf_schema"

    def test_bigquery(self):
        result = build_config_dict_from_db_params(
            db_type="bigquery",
            database="bq_dataset",
            schema="bq_schema",
            project_id="my-project-123",
        )
        assert result[CONFIG_DWH_DIALECT] == "bigquery"
        assert result[CONFIG_DWH_PROJECT_ID] == "my-project-123"
        assert result[CONFIG_DWH_SCHEMA] == "bq_schema"

    def test_model_path(self):
        result = build_config_dict_from_db_params(
            db_type="mysql",
            host="localhost",
            model_path="/path/to/models",
        )
        assert result[CONFIG_MODEL_PATH] == "/path/to/models"

    def test_model_path_not_set_when_empty(self):
        result = build_config_dict_from_db_params(db_type="mysql")
        assert CONFIG_MODEL_PATH not in result

    def test_explicit_schema_overrides_default(self):
        result = build_config_dict_from_db_params(
            db_type="mysql",
            schema="custom_schema",
        )
        assert result[CONFIG_DWH_SCHEMA] == "custom_schema"

    def test_unknown_db_type_passes_through(self):
        result = build_config_dict_from_db_params(db_type="mssql")
        assert result[CONFIG_DWH_DIALECT] == "mssql"
        assert result[CONFIG_DWH_SCHEMA] == ""

    def test_clickhouse(self):
        result = build_config_dict_from_db_params(
            db_type="clickhouse",
            host="ch-host",
            port="8123",
            database="ch_db",
        )
        assert result[CONFIG_DWH_DIALECT] == "clickhouse"


class TestDictConfigHandler:
    """Tests for DictConfigHandler."""

    def test_get_value_returns_from_dict(self):
        handler = DictConfigHandler({
            CONFIG_DWH_DIALECT: "mysql",
            CONFIG_DWH_HOST: "localhost",
        })
        assert handler.get_value(CONFIG_DWH_DIALECT) == "mysql"
        assert handler.get_value(CONFIG_DWH_HOST) == "localhost"

    def test_get_value_returns_none_for_missing_key(self):
        handler = DictConfigHandler({})
        assert handler.get_value(CONFIG_DWH_HOST) is None

    def test_properties(self):
        handler = DictConfigHandler({})
        assert handler.dir_path == str(pathlib.Path.home() / ".metricflow")
        assert handler.file_path.endswith("dict_config_dummy.yml")
        assert handler.log_file_path.endswith("metricflow.log")

    def test_round_trip_with_build_function(self):
        """Verify DictConfigHandler + build_config_dict_from_db_params work together."""
        config_dict = build_config_dict_from_db_params(
            db_type="mysql",
            host="db-host",
            port="3306",
            username="user1",
            password="pass1",
            database="testdb",
            model_path="/models",
        )
        handler = DictConfigHandler(config_dict)
        assert handler.get_value(CONFIG_DWH_DIALECT) == "mysql"
        assert handler.get_value(CONFIG_DWH_HOST) == "db-host"
        assert handler.get_value(CONFIG_DWH_PORT) == "3306"
        assert handler.get_value(CONFIG_DWH_USER) == "user1"
        assert handler.get_value(CONFIG_DWH_PASSWORD) == "pass1"
        assert handler.get_value(CONFIG_DWH_DB) == "testdb"
        assert handler.get_value(CONFIG_DWH_SCHEMA) == "default"
        assert handler.get_value(CONFIG_MODEL_PATH) == "/models"
