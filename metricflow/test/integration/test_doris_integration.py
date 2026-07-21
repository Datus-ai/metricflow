"""Live end-to-end tests for Apache Doris.

Set DORIS_HOST, DORIS_PORT, DORIS_USER, DORIS_PASSWORD, and DORIS_DATABASE
when the test database is not available at the local defaults.
"""

import os
import shutil
import tempfile
from string import Template

import pandas as pd
import pytest
import sqlalchemy.exc

from metricflow.cli.tutorial import (
    COUNTRIES_TABLE,
    COUNTRIES_YAML_FILE,
    CUSTOMERS_TABLE,
    CUSTOMERS_YAML_FILE,
    TRANSACTIONS_TABLE,
    TRANSACTIONS_YAML_FILE,
    create_sample_data,
    remove_sample_tables,
)
from metricflow.configuration.dict_config_handler import DictConfigHandler, build_config_dict_from_db_params
from metricflow.dataflow.sql_table import SqlTable
from metricflow.engine.metricflow_engine import MetricFlowEngine, MetricFlowQueryRequest
from metricflow.engine.utils import model_build_result_from_config
from metricflow.model.data_warehouse_model_validator import DataWarehouseModelValidator
from metricflow.model.model_validator import ModelValidator
from metricflow.model.validations.validator_helpers import ModelValidationResults
from metricflow.object_utils import random_id
from metricflow.protocols.sql_client import SqlEngine
from metricflow.sql.render.doris import DorisSqlQueryPlanRenderer
from metricflow.sql_clients.doris import DorisSqlClient
from metricflow.sql_clients.sql_utils import make_sql_client_from_config


DORIS_HOST = os.getenv("DORIS_HOST", "localhost")
DORIS_PORT = os.getenv("DORIS_PORT", "9030")
DORIS_USER = os.getenv("DORIS_USER", "root")
DORIS_PASSWORD = os.getenv("DORIS_PASSWORD", "")
DORIS_DATABASE = os.getenv("DORIS_DATABASE", "test")
DORIS_URL = f"doris://{DORIS_USER}@{DORIS_HOST}:{DORIS_PORT}/{DORIS_DATABASE}"


def _make_doris_client():
    try:
        client = DorisSqlClient.from_connection_details(DORIS_URL, DORIS_PASSWORD)
        client.query("SELECT 1")
        return client
    except (sqlalchemy.exc.OperationalError, ConnectionError, OSError):
        return None


@pytest.fixture(scope="module")
def doris_client():
    client = _make_doris_client()
    if client is None:
        pytest.skip(f"Doris is not available at {DORIS_HOST}:{DORIS_PORT}/{DORIS_DATABASE}")
    yield client
    client.close()


@pytest.fixture(scope="module")
def doris_model_dir():
    model_dir = tempfile.mkdtemp(prefix="doris_test_models_")
    for yaml_file, table_name in (
        (TRANSACTIONS_YAML_FILE, f"{DORIS_DATABASE}.{TRANSACTIONS_TABLE}"),
        (CUSTOMERS_YAML_FILE, f"{DORIS_DATABASE}.{CUSTOMERS_TABLE}"),
        (COUNTRIES_YAML_FILE, f"{DORIS_DATABASE}.{COUNTRIES_TABLE}"),
    ):
        with open(yaml_file) as source:
            variable = os.path.basename(yaml_file).replace(".yaml", "_table")
            contents = Template(source.read()).substitute({variable: table_name})
        with open(os.path.join(model_dir, os.path.basename(yaml_file)), "w") as destination:
            destination.write(contents)
    yield model_dir
    shutil.rmtree(model_dir, ignore_errors=True)


def _handler(model_dir: str) -> DictConfigHandler:
    return DictConfigHandler(
        build_config_dict_from_db_params(
            db_type="doris",
            host=DORIS_HOST,
            port=DORIS_PORT,
            username=DORIS_USER,
            password=DORIS_PASSWORD,
            database=DORIS_DATABASE,
            model_path=model_dir,
        )
    )


@pytest.fixture(scope="module")
def doris_sample_data(doris_client):
    remove_sample_tables(sql_client=doris_client, system_schema=DORIS_DATABASE)
    assert create_sample_data(sql_client=doris_client, system_schema=DORIS_DATABASE)
    yield
    remove_sample_tables(sql_client=doris_client, system_schema=DORIS_DATABASE)


@pytest.mark.doris
class TestDorisDatabaseOperations:
    def test_engine_attributes(self, doris_client) -> None:
        attributes = doris_client.sql_engine_attributes
        assert attributes.sql_engine_type is SqlEngine.DORIS
        assert isinstance(attributes.sql_query_plan_renderer, DorisSqlQueryPlanRenderer)

    def test_health_checks(self, doris_client) -> None:
        for name, result in doris_client.health_checks(schema_name=DORIS_DATABASE).items():
            assert result["status"] == "SUCCESS", f"{name}: {result}"

    def test_query_and_dry_run(self, doris_client) -> None:
        result = doris_client.query("SELECT 1 AS value")
        assert result.iloc[0]["value"] == 1
        doris_client.dry_run("SELECT 1")

    def test_create_list_and_drop_table(self, doris_client) -> None:
        table = SqlTable(schema_name=DORIS_DATABASE, table_name=f"doris_test_{random_id()}")
        try:
            doris_client.create_table_as_select(table, "SELECT 42 AS answer, 'hello' AS greeting")
            result = doris_client.query(f"SELECT * FROM {table.sql}")
            assert len(result) == 1
            assert set(result.columns) == {"answer", "greeting"}
            assert table.table_name in doris_client.list_tables(DORIS_DATABASE)
        finally:
            doris_client.execute(f"DROP TABLE IF EXISTS {table.sql}")

    def test_create_table_from_dataframe_with_bound_values(self, doris_client) -> None:
        table = SqlTable(schema_name=DORIS_DATABASE, table_name=f"doris_dataframe_test_{random_id()}")
        dataframe = pd.DataFrame(
            {
                "name": ["O'Reilly\\", None],
                "count_value": pd.Series([1, pd.NA], dtype="Int64"),
                "enabled": pd.Series([True, pd.NA], dtype="boolean"),
            }
        )
        try:
            doris_client.create_table_from_dataframe(table, dataframe)

            populated = doris_client.query(f"SELECT name, count_value, enabled FROM {table.sql} WHERE count_value = 1")
            assert populated.iloc[0]["name"] == "O'Reilly\\"
            assert populated.iloc[0]["count_value"] == 1
            assert bool(populated.iloc[0]["enabled"])

            null_row = doris_client.query(
                f"SELECT name, count_value, enabled FROM {table.sql} WHERE count_value IS NULL"
            )
            assert null_row.iloc[0].isna().all()
        finally:
            doris_client.execute(f"DROP TABLE IF EXISTS {table.sql}")


@pytest.mark.doris
class TestDorisMetricFlowPipeline:
    def test_config_factory(self, doris_client, doris_model_dir) -> None:
        client = make_sql_client_from_config(_handler(doris_model_dir))
        try:
            assert isinstance(client, DorisSqlClient)
            assert client.query("SELECT 1").iloc[0, 0] == 1
        finally:
            client.close()

    def test_model_and_warehouse_validation(self, doris_client, doris_model_dir, doris_sample_data) -> None:
        build_result = model_build_result_from_config(
            handler=_handler(doris_model_dir), raise_issues_as_exceptions=False
        )
        assert not build_result.issues.has_blocking_issues, build_result.issues.summary()

        semantic_result = ModelValidator().validate_model(build_result.model)
        assert not semantic_result.issues.has_blocking_issues, semantic_result.issues.summary()

        validator = DataWarehouseModelValidator(sql_client=doris_client, system_schema=DORIS_DATABASE)
        results = [
            validate(build_result.model, None)
            for validate in (
                validator.validate_data_sources,
                validator.validate_dimensions,
                validator.validate_identifiers,
                validator.validate_measures,
            )
        ]
        merged = ModelValidationResults.merge(results)
        assert not merged.has_blocking_issues, merged.summary()

    def test_query_metric(self, doris_model_dir, doris_sample_data) -> None:
        engine = MetricFlowEngine.from_config(_handler(doris_model_dir))
        metrics = {metric.name for metric in engine.list_metrics()}
        assert "transactions" in metrics

        request = MetricFlowQueryRequest.create_with_random_request_id(
            metric_names=["transactions"],
            group_by_names=["metric_time"],
            order_by_names=["metric_time"],
            limit=5,
        )
        explain_result = engine.explain(mf_request=request)
        assert "SELECT" in explain_result.rendered_sql_without_descriptions.sql_query

        result = engine.query(mf_request=request).result_df
        assert result is not None
        assert len(result) > 0
        assert {"transactions", "metric_time"}.issubset(result.columns)
