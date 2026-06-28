import pathlib

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
    CONFIG_MODEL_PATH,
)
from metricflow.configuration.datus_config_handler import DatusConfigHandler
from metricflow.configuration.dict_config_handler import (
    DictConfigHandler,
    build_config_dict_from_datus_datasource,
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
        assert result[CONFIG_DWH_SCHEMA] == "mydb"

    def test_starrocks(self):
        result = build_config_dict_from_db_params(
            db_type="starrocks",
            host="sr-host",
            port="9030",
            username="admin",
            password="pw",
            database="sr_db",
        )
        assert result[CONFIG_DWH_DIALECT] == "starrocks"
        assert result[CONFIG_DWH_SCHEMA] == "sr_db"

    def test_postgresql(self):
        result = build_config_dict_from_db_params(
            db_type="postgresql",
            host="pg-host",
            port="5432",
            username="pguser",
            password="pgpw",
            database="pgdb",
            sslmode="disable",
        )
        assert result[CONFIG_DWH_DIALECT] == "postgresql"
        assert result[CONFIG_DWH_SCHEMA] == "public"
        assert result[CONFIG_DWH_SSLMODE] == "disable"

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
            role="analytics_role",
            private_key="inline-private-key",
            private_key_file="/tmp/rsa_key.p8",
            private_key_file_pwd="key-pass",
        )
        assert result[CONFIG_DWH_DIALECT] == "snowflake"
        assert result[CONFIG_DWH_WAREHOUSE] == "wh1"
        assert result[CONFIG_DWH_ACCOUNT] == "my_account"
        assert result[CONFIG_DWH_ROLE] == "analytics_role"
        assert result[CONFIG_DWH_PRIVATE_KEY] == "inline-private-key"
        assert result[CONFIG_DWH_PRIVATE_KEY_FILE] == "/tmp/rsa_key.p8"
        assert result[CONFIG_DWH_PRIVATE_KEY_FILE_PWD] == "key-pass"
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

    def test_positional_arguments_preserve_existing_order(self):
        result = build_config_dict_from_db_params(
            "bigquery",
            "",
            "",
            "",
            "",
            "bq_dataset",
            "bq_schema",
            "",
            "",
            "",
            "my-project-123",
            "/path/to/models",
            "disable",
        )

        assert result[CONFIG_DWH_PROJECT_ID] == "my-project-123"
        assert result[CONFIG_MODEL_PATH] == "/path/to/models"
        assert result[CONFIG_DWH_SSLMODE] == "disable"

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
            database="mydb",
            schema="custom_schema",
        )
        assert result[CONFIG_DWH_SCHEMA] == "custom_schema"

    def test_mysql_without_database_keeps_legacy_default_schema(self):
        result = build_config_dict_from_db_params(db_type="mysql")
        assert result[CONFIG_DWH_SCHEMA] == "default"

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

    def test_trino_catalog_maps_to_dwh_database_and_database_maps_to_schema(self):
        result = build_config_dict_from_db_params(
            db_type="trino",
            catalog="hive",
            database="college_exam",
        )
        assert result[CONFIG_DWH_DB] == "hive"
        assert result[CONFIG_DWH_SCHEMA] == "college_exam"


class TestBuildConfigDictFromDatusDatasource:
    def test_builds_config_from_raw_snowflake_datasource(self):
        result = build_config_dict_from_datus_datasource(
            {
                "type": "snowflake",
                "account": "sf_account",
                "username": "sf_user",
                "database": "sf_db",
                "schema_name": "sf_schema",
                "warehouse": "wh1",
                "role": "analyst",
                "private_key": "inline-private-key",
                "private_key_file_pwd": 1234,
            },
            model_path="/tmp/models",
        )

        assert result[CONFIG_DWH_DIALECT] == "snowflake"
        assert result[CONFIG_DWH_ACCOUNT] == "sf_account"
        assert result[CONFIG_DWH_USER] == "sf_user"
        assert result[CONFIG_DWH_DB] == "sf_db"
        assert result[CONFIG_DWH_SCHEMA] == "sf_schema"
        assert result[CONFIG_DWH_WAREHOUSE] == "wh1"
        assert result[CONFIG_DWH_ROLE] == "analyst"
        assert result[CONFIG_DWH_PRIVATE_KEY] == "inline-private-key"
        assert result[CONFIG_DWH_PRIVATE_KEY_FILE_PWD] == "1234"
        assert result[CONFIG_MODEL_PATH] == "/tmp/models"

    def test_builds_config_from_raw_postgres_datasource(self):
        result = build_config_dict_from_datus_datasource(
            {
                "type": "postgres",
                "host": "localhost",
                "port": 15432,
                "username": "datus",
                "password": "secret",
                "database": "analytics",
                "schema": "public",
                "sslmode": "require",
            },
            model_path="/tmp/models",
        )

        assert result[CONFIG_DWH_DIALECT] == "postgresql"
        assert result[CONFIG_DWH_HOST] == "localhost"
        assert result[CONFIG_DWH_PORT] == "15432"
        assert result[CONFIG_DWH_USER] == "datus"
        assert result[CONFIG_DWH_PASSWORD] == "secret"
        assert result[CONFIG_DWH_DB] == "analytics"
        assert result[CONFIG_DWH_SCHEMA] == "public"
        assert result[CONFIG_DWH_SSLMODE] == "require"
        assert result[CONFIG_MODEL_PATH] == "/tmp/models"

    def test_builds_config_from_runtime_trino_context(self):
        result = build_config_dict_from_datus_datasource(
            {
                "type": "trino",
                "host": "trino-host",
                "port": 8080,
                "username": "trino",
                "catalog": "hive",
                "database": "college_exam",
            }
        )

        assert result[CONFIG_DWH_DIALECT] == "trino"
        assert result[CONFIG_DWH_DB] == "hive"
        assert result[CONFIG_DWH_SCHEMA] == "college_exam"

    def test_builds_config_from_runtime_schema_alias(self):
        result = build_config_dict_from_datus_datasource(
            {
                "type": "mysql",
                "host": "mysql-host",
                "database": "college_exam",
                "db_schema": "semantic_schema",
            }
        )

        assert result[CONFIG_DWH_DB] == "college_exam"
        assert result[CONFIG_DWH_SCHEMA] == "semantic_schema"


class TestDictConfigHandler:
    """Tests for DictConfigHandler."""

    def test_get_value_returns_from_dict(self):
        handler = DictConfigHandler(
            {
                CONFIG_DWH_DIALECT: "mysql",
                CONFIG_DWH_HOST: "localhost",
            }
        )
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
        assert handler.get_value(CONFIG_DWH_SCHEMA) == "testdb"
        assert handler.get_value(CONFIG_MODEL_PATH) == "/models"


class TestDatusConfigHandler:
    def test_get_value_returns_sslmode_from_datasource_config(self, tmp_path):
        config_path = tmp_path / "agent.yml"
        config_path.write_text(
            """
agent:
  services:
    datasources:
      greenplum:
        type: greenplum
        host: gp-master
        port: 5432
        username: gpadmin
        password: secret
        database: testdb
        sslmode: disable
""",
            encoding="utf-8",
        )

        handler = DatusConfigHandler(
            datasource="greenplum",
            config_path=str(config_path),
            project_root=str(tmp_path),
        )

        assert handler.get_value(CONFIG_DWH_SSLMODE) == "disable"

    def test_get_value_returns_snowflake_key_pair_fields(self, tmp_path, monkeypatch):
        key_file = tmp_path / "rsa_key.p8"
        monkeypatch.setenv("SNOWFLAKE_KEY_FILE", str(key_file))
        monkeypatch.setenv("SNOWFLAKE_PRIVATE_KEY", "inline-private-key")

        config_path = tmp_path / "agent.yml"
        config_path.write_text(
            """
agent:
  services:
    datasources:
      snowflake:
        type: snowflake
        account: sf_account
        username: sf_user
        role: analyst
        private_key: ${SNOWFLAKE_PRIVATE_KEY}
        private_key_file: ${SNOWFLAKE_KEY_FILE}
        private_key_file_pwd: 1234
        warehouse: wh1
        database: sf_db
""",
            encoding="utf-8",
        )

        handler = DatusConfigHandler(
            datasource="snowflake",
            config_path=str(config_path),
            project_root=str(tmp_path),
        )

        assert handler.get_value(CONFIG_DWH_ROLE) == "analyst"
        assert handler.get_value(CONFIG_DWH_PRIVATE_KEY) == "inline-private-key"
        assert handler.get_value(CONFIG_DWH_PRIVATE_KEY_FILE) == str(key_file)
        assert handler.get_value(CONFIG_DWH_PRIVATE_KEY_FILE_PWD) == "1234"
