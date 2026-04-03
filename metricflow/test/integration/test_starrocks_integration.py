"""End-to-end integration tests for the StarRocks pipeline.

Tests the full flow: config -> client creation -> SQL rendering -> query execution.
- Tests marked with no marker run without a database (mock-based).
- Tests marked with @pytest.mark.starrocks require a real StarRocks instance.
  Uses Docker env from datus-starrocks: localhost:9030, root/(empty password).
"""
import os
import shutil
import tempfile
from string import Template

import pytest
from unittest.mock import patch, MagicMock

from metricflow.configuration.constants import (
    CONFIG_DWH_DB,
    CONFIG_DWH_DIALECT,
    CONFIG_DWH_HOST,
    CONFIG_DWH_PASSWORD,
    CONFIG_DWH_PORT,
    CONFIG_DWH_SCHEMA,
    CONFIG_DWH_USER,
    CONFIG_MODEL_PATH,
)
from metricflow.configuration.dict_config_handler import (
    DictConfigHandler,
    build_config_dict_from_db_params,
    DIALECT_MAPPING,
)
from metricflow.engine.metricflow_engine import MetricFlowQueryRequest
from metricflow.protocols.sql_client import SqlEngine
from metricflow.sql.render.starrocks import StarRocksSqlQueryPlanRenderer
from metricflow.sql_clients.common_client import SqlDialect
from metricflow.sql_clients.starrocks import StarRocksEngineAttributes, StarRocksSqlClient
from metricflow.sql_clients.sql_utils import make_sql_client_from_config


# ---------------------------------------------------------------------------
# Config layer tests (no database needed)
# ---------------------------------------------------------------------------


class TestStarRocksDictConfigHandler:
    """Tests that DictConfigHandler correctly handles starrocks config."""

    def test_dialect_mapping_contains_starrocks(self) -> None:
        assert "starrocks" in DIALECT_MAPPING
        assert DIALECT_MAPPING["starrocks"] == "starrocks"

    def test_build_config_dict_starrocks(self) -> None:
        config = build_config_dict_from_db_params(
            db_type="starrocks",
            host="sr-host",
            port="9030",
            username="root",
            password="",
            database="test",
        )
        assert config[CONFIG_DWH_DIALECT] == "starrocks"
        assert config[CONFIG_DWH_HOST] == "sr-host"
        assert config[CONFIG_DWH_PORT] == "9030"
        assert config[CONFIG_DWH_USER] == "root"
        assert config[CONFIG_DWH_PASSWORD] == ""
        assert config[CONFIG_DWH_DB] == "test"
        # StarRocks uses database as schema
        assert config[CONFIG_DWH_SCHEMA] == "test"

    def test_build_config_dict_starrocks_custom_schema(self) -> None:
        config = build_config_dict_from_db_params(
            db_type="starrocks",
            host="sr-host",
            port="9030",
            username="root",
            password="",
            database="test",
            schema="analytics",
        )
        assert config[CONFIG_DWH_SCHEMA] == "analytics"

    def test_dict_config_handler_get_value(self) -> None:
        config_dict = build_config_dict_from_db_params(
            db_type="starrocks",
            host="sr-host",
            port="9030",
            username="root",
            password="",
            database="test",
        )
        handler = DictConfigHandler(config_dict)
        assert handler.get_value(CONFIG_DWH_DIALECT) == "starrocks"
        assert handler.get_value(CONFIG_DWH_HOST) == "sr-host"
        assert handler.get_value(CONFIG_DWH_DB) == "test"


class TestStarRocksClientFromConfig:
    """Tests that make_sql_client_from_config creates StarRocksSqlClient for starrocks dialect."""

    def test_make_sql_client_from_config_creates_starrocks_client(self) -> None:
        config_dict = build_config_dict_from_db_params(
            db_type="starrocks",
            host="sr-host",
            port="9030",
            username="root",
            password="",
            database="test",
        )
        handler = DictConfigHandler(config_dict)

        with patch.object(
            StarRocksSqlClient, "from_connection_details", return_value=MagicMock()
        ) as mock_factory:
            make_sql_client_from_config(handler)
            mock_factory.assert_called_once()
            call_args = mock_factory.call_args
            url = call_args[0][0]
            password = call_args[0][1]
            assert "starrocks://" in url
            assert "root" in url
            assert "sr-host" in url
            assert "9030" in url
            assert "test" in url
            assert password == ""


class TestStarRocksSqlRendering:
    """Tests that the StarRocks renderer produces correct SQL for full query plans."""

    def test_renderer_renders_simple_select(self) -> None:
        from metricflow.dataflow.sql_table import SqlTable
        from metricflow.sql.sql_exprs import SqlColumnReferenceExpression, SqlColumnReference
        from metricflow.sql.sql_plan import (
            SqlQueryPlan,
            SqlSelectColumn,
            SqlSelectStatementNode,
            SqlTableFromClauseNode,
        )

        select_node = SqlSelectStatementNode(
            description="StarRocks simple query",
            select_columns=(
                SqlSelectColumn(
                    expr=SqlColumnReferenceExpression(SqlColumnReference("a", "revenue")),
                    column_alias="revenue",
                ),
            ),
            from_source=SqlTableFromClauseNode(sql_table=SqlTable(schema_name="test", table_name="sales")),
            from_source_alias="a",
            joins_descs=(),
            where=None,
            group_bys=(),
            order_bys=(),
        )
        plan = SqlQueryPlan(plan_id="sr_test", render_node=select_node)
        renderer = StarRocksSqlQueryPlanRenderer()
        rendered = renderer.render_sql_query_plan(plan)
        assert "SELECT" in rendered.sql
        assert "a.revenue" in rendered.sql
        assert "test.sales" in rendered.sql

    def test_engine_attributes_renderer_is_starrocks(self) -> None:
        renderer = StarRocksEngineAttributes.sql_query_plan_renderer
        assert isinstance(renderer, StarRocksSqlQueryPlanRenderer)


class TestStarRocksCliSetupDialect:
    """Tests that the CLI setup command includes starrocks in dialect choices."""

    def test_starrocks_in_cli_dialect_map(self) -> None:
        from metricflow.cli.utils import MF_STARROCKS_KEYS
        assert any(
            k.key == CONFIG_DWH_DIALECT and k.value == SqlDialect.STARROCKS.value
            for k in MF_STARROCKS_KEYS
        )


# ---------------------------------------------------------------------------
# Live database tests (require a real StarRocks instance)
# Uses Docker environment from datus-starrocks: localhost:9030, root/(empty)
# ---------------------------------------------------------------------------

SR_URL = "starrocks://root@localhost:9030/test"
SR_PASSWORD = ""


def _make_sr_client():
    """Create a StarRocksSqlClient connected to the Docker instance."""
    try:
        client = StarRocksSqlClient.from_connection_details(url=SR_URL, password=SR_PASSWORD)
        client.query("SELECT 1")
        return client
    except Exception:
        return None


@pytest.fixture(scope="module")
def sr_client():
    """Module-scoped fixture for a live StarRocks client."""
    client = _make_sr_client()
    if client is None:
        pytest.skip("StarRocks Docker not available at localhost:9030")
    yield client
    client.close()


@pytest.mark.starrocks
class TestStarRocksLiveDatabase:
    """Integration tests that require a real StarRocks database."""

    def test_health_checks(self, sr_client):
        result = sr_client.health_checks(schema_name="test")
        assert isinstance(result, dict)
        for check_name, check_result in result.items():
            assert check_result["status"] == "SUCCESS", f"{check_name} failed: {check_result}"

    def test_simple_query(self, sr_client):
        df = sr_client.query("SELECT 1 AS val")
        assert len(df) == 1

    def test_create_table_and_query(self, sr_client):
        from metricflow.dataflow.sql_table import SqlTable
        from metricflow.object_utils import random_id

        table = SqlTable(schema_name="test", table_name=f"sr_test_{random_id()}")
        try:
            sr_client.create_table_as_select(table, "SELECT 42 AS answer, 'hello' AS greeting")
            df = sr_client.query(f"SELECT * FROM {table.sql}")
            assert len(df) == 1
            assert set(df.columns) == {"answer", "greeting"}
        finally:
            sr_client.execute(f"DROP TABLE IF EXISTS {table.sql}")

    def test_validate_configs_with_starrocks(self, sr_client):
        attrs = sr_client.sql_engine_attributes
        assert attrs.sql_engine_type == SqlEngine.STARROCKS
        assert attrs.date_trunc_supported is False
        assert attrs.full_outer_joins_supported is False
        assert isinstance(attrs.sql_query_plan_renderer, StarRocksSqlQueryPlanRenderer)

    def test_list_tables(self, sr_client):
        from metricflow.dataflow.sql_table import SqlTable
        from metricflow.object_utils import random_id

        table = SqlTable(schema_name="test", table_name=f"sr_list_{random_id()}")
        try:
            sr_client.create_table_as_select(table, "SELECT 1 AS x")
            tables = sr_client.list_tables("test")
            assert table.table_name in tables
        finally:
            sr_client.execute(f"DROP TABLE IF EXISTS {table.sql}")


# ---------------------------------------------------------------------------
# Helper: build DictConfigHandler with model_path for SR Docker env
# ---------------------------------------------------------------------------

SR_SCHEMA = "test"


def _build_sr_handler_with_models(model_dir: str) -> DictConfigHandler:
    config_dict = build_config_dict_from_db_params(
        db_type="starrocks",
        host="localhost",
        port="9030",
        username="root",
        password="",
        database="test",
        schema=SR_SCHEMA,
        model_path=model_dir,
    )
    return DictConfigHandler(config_dict)


def _generate_sample_model_configs(model_dir: str) -> None:
    from metricflow.cli.tutorial import (
        TRANSACTIONS_YAML_FILE,
        CUSTOMERS_YAML_FILE,
        COUNTRIES_YAML_FILE,
        TRANSACTIONS_TABLE,
        CUSTOMERS_TABLE,
        COUNTRIES_TABLE,
    )

    for yaml_file, table_name in [
        (TRANSACTIONS_YAML_FILE, f"{SR_SCHEMA}.{TRANSACTIONS_TABLE}"),
        (CUSTOMERS_YAML_FILE, f"{SR_SCHEMA}.{CUSTOMERS_TABLE}"),
        (COUNTRIES_YAML_FILE, f"{SR_SCHEMA}.{COUNTRIES_TABLE}"),
    ]:
        with open(yaml_file) as f:
            var_name = os.path.basename(yaml_file).replace(".yaml", "_table")
            contents = Template(f.read()).substitute({var_name: table_name})
        with open(os.path.join(model_dir, os.path.basename(yaml_file)), "w") as f:
            f.write(contents)


@pytest.fixture(scope="module")
def sr_model_dir():
    d = tempfile.mkdtemp(prefix="sr_test_models_")
    _generate_sample_model_configs(d)
    yield d
    shutil.rmtree(d, ignore_errors=True)


@pytest.fixture(scope="module")
def sr_sample_data(sr_client):
    from metricflow.cli.tutorial import create_sample_data, remove_sample_tables

    remove_sample_tables(sql_client=sr_client, system_schema=SR_SCHEMA)
    created = create_sample_data(sql_client=sr_client, system_schema=SR_SCHEMA)
    assert created, "Failed to create sample data"
    yield
    remove_sample_tables(sql_client=sr_client, system_schema=SR_SCHEMA)


@pytest.mark.starrocks
class TestStarRocksValidateConfigs:
    """Test the full validate-configs pipeline against a live StarRocks database."""

    def test_health_checks_via_config(self, sr_client, sr_model_dir):
        handler = _build_sr_handler_with_models(sr_model_dir)
        sql_client = make_sql_client_from_config(handler)
        results = sql_client.health_checks(schema_name=SR_SCHEMA)
        for name, result in results.items():
            assert result["status"] == "SUCCESS", f"{name}: {result}"

    def test_model_build_from_config(self, sr_model_dir):
        from metricflow.engine.utils import model_build_result_from_config

        handler = _build_sr_handler_with_models(sr_model_dir)
        build_result = model_build_result_from_config(
            handler=handler, raise_issues_as_exceptions=False
        )
        assert not build_result.issues.has_blocking_issues, (
            f"Model build had blocking issues: {build_result.issues.summary()}"
        )

    def test_semantic_validation(self, sr_model_dir):
        from metricflow.engine.utils import model_build_result_from_config
        from metricflow.model.model_validator import ModelValidator

        handler = _build_sr_handler_with_models(sr_model_dir)
        build_result = model_build_result_from_config(
            handler=handler, raise_issues_as_exceptions=False
        )
        semantic_result = ModelValidator().validate_model(build_result.model)
        assert not semantic_result.issues.has_blocking_issues, (
            f"Semantic validation had blocking issues: {semantic_result.issues.summary()}"
        )

    def test_data_warehouse_validation(self, sr_client, sr_model_dir, sr_sample_data):
        from metricflow.engine.utils import model_build_result_from_config
        from metricflow.model.data_warehouse_model_validator import DataWarehouseModelValidator
        from metricflow.model.validations.validator_helpers import ModelValidationResults

        handler = _build_sr_handler_with_models(sr_model_dir)
        build_result = model_build_result_from_config(
            handler=handler, raise_issues_as_exceptions=False
        )
        model = build_result.model
        dw_validator = DataWarehouseModelValidator(
            sql_client=sr_client, system_schema=SR_SCHEMA
        )
        results = []
        for validate_fn in [
            dw_validator.validate_data_sources,
            dw_validator.validate_dimensions,
            dw_validator.validate_identifiers,
            dw_validator.validate_measures,
        ]:
            result = validate_fn(model, None)
            results.append(result)
        merged = ModelValidationResults.merge(results)
        assert not merged.has_blocking_issues, (
            f"DW validation had blocking issues: {merged.summary()}"
        )


@pytest.mark.starrocks
class TestStarRocksQueryMetrics:
    """Test the full query --metrics pipeline against a live StarRocks database."""

    def test_build_engine_from_config(self, sr_client, sr_model_dir, sr_sample_data):
        from metricflow.engine.metricflow_engine import MetricFlowEngine

        handler = _build_sr_handler_with_models(sr_model_dir)
        engine = MetricFlowEngine.from_config(handler)
        assert engine is not None

    def test_list_metrics(self, sr_client, sr_model_dir, sr_sample_data):
        from metricflow.engine.metricflow_engine import MetricFlowEngine

        handler = _build_sr_handler_with_models(sr_model_dir)
        engine = MetricFlowEngine.from_config(handler)
        metrics = engine.list_metrics()
        metric_names = [m.name for m in metrics]
        assert "transactions" in metric_names

    def test_explain_query(self, sr_client, sr_model_dir, sr_sample_data):
        from metricflow.engine.metricflow_engine import MetricFlowEngine

        handler = _build_sr_handler_with_models(sr_model_dir)
        engine = MetricFlowEngine.from_config(handler)

        mf_request = MetricFlowQueryRequest.create_with_random_request_id(
            metric_names=["transactions"],
            group_by_names=["metric_time"],
            order_by_names=["metric_time"],
            limit=5,
        )
        explain_result = engine.explain(mf_request=mf_request)
        sql = explain_result.rendered_sql_without_descriptions.sql_query
        assert "SELECT" in sql

    def test_query_transactions_metric(self, sr_client, sr_model_dir, sr_sample_data):
        from metricflow.engine.metricflow_engine import MetricFlowEngine

        handler = _build_sr_handler_with_models(sr_model_dir)
        engine = MetricFlowEngine.from_config(handler)

        mf_request = MetricFlowQueryRequest.create_with_random_request_id(
            metric_names=["transactions"],
            group_by_names=["metric_time"],
            order_by_names=["metric_time"],
            limit=5,
        )
        result = engine.query(mf_request=mf_request)
        df = result.result_df
        assert df is not None
        assert len(df) > 0
        assert "transactions" in df.columns
        assert "metric_time" in df.columns

    def test_query_with_dimension_filter(self, sr_client, sr_model_dir, sr_sample_data):
        from metricflow.engine.metricflow_engine import MetricFlowEngine

        handler = _build_sr_handler_with_models(sr_model_dir)
        engine = MetricFlowEngine.from_config(handler)

        mf_request = MetricFlowQueryRequest.create_with_random_request_id(
            metric_names=["transactions"],
            group_by_names=["metric_time"],
            order_by_names=["metric_time"],
            where_constraint="is_large = true",
        )
        result = engine.query(mf_request=mf_request)
        df = result.result_df
        assert df is not None
        assert len(df) > 0
