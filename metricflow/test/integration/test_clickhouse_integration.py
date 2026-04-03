"""End-to-end integration tests for the ClickHouse pipeline.

Tests the full flow: config -> client creation -> SQL rendering -> query execution.
- Tests marked with no marker run without a database (mock-based).
- Tests marked with @pytest.mark.clickhouse require a real ClickHouse instance.
  Uses Docker env from datus-clickhouse: localhost:8123, default_user/default_test.
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
from metricflow.sql.render.clickhouse import ClickHouseSqlQueryPlanRenderer
from metricflow.sql_clients.common_client import SqlDialect
from metricflow.sql_clients.clickhouse import ClickHouseEngineAttributes, ClickHouseSqlClient
from metricflow.sql_clients.sql_utils import make_sql_client_from_config


# ---------------------------------------------------------------------------
# Config layer tests (no database needed)
# ---------------------------------------------------------------------------


class TestClickHouseDictConfigHandler:
    """Tests that DictConfigHandler correctly handles clickhouse config."""

    def test_dialect_mapping_contains_clickhouse(self) -> None:
        assert "clickhouse" in DIALECT_MAPPING
        assert DIALECT_MAPPING["clickhouse"] == "clickhouse"

    def test_build_config_dict_clickhouse(self) -> None:
        config = build_config_dict_from_db_params(
            db_type="clickhouse",
            host="ch-host",
            port="8123",
            username="default_user",
            password="default_test",
            database="default_test",
        )
        assert config[CONFIG_DWH_DIALECT] == "clickhouse"
        assert config[CONFIG_DWH_HOST] == "ch-host"
        assert config[CONFIG_DWH_PORT] == "8123"
        assert config[CONFIG_DWH_USER] == "default_user"
        assert config[CONFIG_DWH_PASSWORD] == "default_test"
        assert config[CONFIG_DWH_DB] == "default_test"
        # ClickHouse uses database as schema
        assert config[CONFIG_DWH_SCHEMA] == "default_test"

    def test_build_config_dict_clickhouse_custom_schema(self) -> None:
        config = build_config_dict_from_db_params(
            db_type="clickhouse",
            host="ch-host",
            port="8123",
            username="default_user",
            password="default_test",
            database="default_test",
            schema="analytics",
        )
        assert config[CONFIG_DWH_SCHEMA] == "analytics"

    def test_dict_config_handler_get_value(self) -> None:
        config_dict = build_config_dict_from_db_params(
            db_type="clickhouse",
            host="ch-host",
            port="8123",
            username="default_user",
            password="default_test",
            database="default_test",
        )
        handler = DictConfigHandler(config_dict)
        assert handler.get_value(CONFIG_DWH_DIALECT) == "clickhouse"
        assert handler.get_value(CONFIG_DWH_HOST) == "ch-host"
        assert handler.get_value(CONFIG_DWH_DB) == "default_test"


class TestClickHouseClientFromConfig:
    """Tests that make_sql_client_from_config creates ClickHouseSqlClient for clickhouse dialect."""

    def test_make_sql_client_from_config_creates_clickhouse_client(self) -> None:
        config_dict = build_config_dict_from_db_params(
            db_type="clickhouse",
            host="ch-host",
            port="8123",
            username="default_user",
            password="default_test",
            database="default_test",
        )
        handler = DictConfigHandler(config_dict)

        with patch.object(
            ClickHouseSqlClient, "from_connection_details", return_value=MagicMock()
        ) as mock_factory:
            make_sql_client_from_config(handler)
            mock_factory.assert_called_once()
            call_args = mock_factory.call_args
            url = call_args[0][0]
            password = call_args[0][1]
            assert "clickhouse://" in url
            assert "default_user" in url
            assert "ch-host" in url
            assert "8123" in url
            assert "default_test" in url
            assert password == "default_test"


class TestClickHouseSqlRendering:
    """Tests that the ClickHouse renderer produces correct SQL for full query plans."""

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
            description="ClickHouse simple query",
            select_columns=(
                SqlSelectColumn(
                    expr=SqlColumnReferenceExpression(SqlColumnReference("a", "revenue")),
                    column_alias="revenue",
                ),
            ),
            from_source=SqlTableFromClauseNode(sql_table=SqlTable(schema_name="default", table_name="sales")),
            from_source_alias="a",
            joins_descs=(),
            where=None,
            group_bys=(),
            order_bys=(),
        )
        plan = SqlQueryPlan(plan_id="ch_test", render_node=select_node)
        renderer = ClickHouseSqlQueryPlanRenderer()
        rendered = renderer.render_sql_query_plan(plan)
        assert "SELECT" in rendered.sql
        assert "a.revenue" in rendered.sql
        assert "default.sales" in rendered.sql

    def test_renderer_renders_uuid_with_generateuuidv4(self) -> None:
        from metricflow.dataflow.sql_table import SqlTable
        from metricflow.sql.sql_exprs import SqlGenerateUuidExpression
        from metricflow.sql.sql_plan import (
            SqlQueryPlan,
            SqlSelectColumn,
            SqlSelectStatementNode,
            SqlTableFromClauseNode,
        )

        select_node = SqlSelectStatementNode(
            description="ClickHouse UUID query",
            select_columns=(
                SqlSelectColumn(
                    expr=SqlGenerateUuidExpression(),
                    column_alias="uuid",
                ),
            ),
            from_source=SqlTableFromClauseNode(sql_table=SqlTable(schema_name="default", table_name="t")),
            from_source_alias="a",
            joins_descs=(),
            where=None,
            group_bys=(),
            order_bys=(),
        )
        plan = SqlQueryPlan(plan_id="ch_uuid", render_node=select_node)
        renderer = ClickHouseSqlQueryPlanRenderer()
        rendered = renderer.render_sql_query_plan(plan)
        assert "generateUUIDv4()" in rendered.sql

    def test_engine_attributes_renderer_is_clickhouse(self) -> None:
        renderer = ClickHouseEngineAttributes.sql_query_plan_renderer
        assert isinstance(renderer, ClickHouseSqlQueryPlanRenderer)


class TestClickHouseCliSetupDialect:
    """Tests that the CLI setup command includes clickhouse in dialect choices."""

    def test_clickhouse_in_cli_dialect_map(self) -> None:
        from metricflow.cli.utils import MF_CLICKHOUSE_KEYS
        assert any(
            k.key == CONFIG_DWH_DIALECT and k.value == SqlDialect.CLICKHOUSE.value
            for k in MF_CLICKHOUSE_KEYS
        )


# ---------------------------------------------------------------------------
# Live database tests (require a real ClickHouse instance)
# Uses Docker environment from datus-clickhouse: localhost:8123
# ---------------------------------------------------------------------------

CH_URL = "clickhouse://default_user@localhost:8123/default_test"
CH_PASSWORD = "default_test"


def _make_ch_client():
    """Create a ClickHouseSqlClient connected to the Docker instance."""
    try:
        client = ClickHouseSqlClient.from_connection_details(url=CH_URL, password=CH_PASSWORD)
        client.query("SELECT 1")
        return client
    except Exception:
        return None


@pytest.fixture(scope="module")
def ch_client():
    """Module-scoped fixture for a live ClickHouse client."""
    client = _make_ch_client()
    if client is None:
        pytest.skip("ClickHouse Docker not available at localhost:8123")
    yield client
    client.close()


@pytest.mark.clickhouse
class TestClickHouseLiveDatabase:
    """Integration tests that require a real ClickHouse database."""

    def test_health_checks(self, ch_client):
        result = ch_client.health_checks(schema_name="default_test")
        assert isinstance(result, dict)
        for check_name, check_result in result.items():
            assert check_result["status"] == "SUCCESS", f"{check_name} failed: {check_result}"

    def test_simple_query(self, ch_client):
        df = ch_client.query("SELECT 1 AS val")
        assert len(df) == 1

    def test_create_table_and_query(self, ch_client):
        from metricflow.dataflow.sql_table import SqlTable
        from metricflow.object_utils import random_id

        table = SqlTable(schema_name="default_test", table_name=f"ch_test_{random_id()}")
        try:
            ch_client.create_table_as_select(table, "SELECT 42 AS answer, 'hello' AS greeting")
            df = ch_client.query(f"SELECT * FROM {table.sql}")
            assert len(df) == 1
            assert set(df.columns) == {"answer", "greeting"}
        finally:
            ch_client.execute(f"DROP TABLE IF EXISTS {table.sql}")

    def test_validate_configs_with_clickhouse(self, ch_client):
        attrs = ch_client.sql_engine_attributes
        assert attrs.sql_engine_type == SqlEngine.CLICKHOUSE
        assert attrs.date_trunc_supported is False
        assert attrs.full_outer_joins_supported is False
        assert isinstance(attrs.sql_query_plan_renderer, ClickHouseSqlQueryPlanRenderer)

    def test_list_tables(self, ch_client):
        from metricflow.dataflow.sql_table import SqlTable
        from metricflow.object_utils import random_id

        table = SqlTable(schema_name="default_test", table_name=f"ch_list_{random_id()}")
        try:
            ch_client.create_table_as_select(table, "SELECT 1 AS x")
            tables = ch_client.list_tables("default_test")
            assert table.table_name in tables
        finally:
            ch_client.execute(f"DROP TABLE IF EXISTS {table.sql}")


# ---------------------------------------------------------------------------
# Helper: build DictConfigHandler with model_path for CH Docker env
# ---------------------------------------------------------------------------

CH_SCHEMA = "default_test"


def _build_ch_handler_with_models(model_dir: str) -> DictConfigHandler:
    config_dict = build_config_dict_from_db_params(
        db_type="clickhouse",
        host="localhost",
        port="8123",
        username="default_user",
        password="default_test",
        database="default_test",
        schema=CH_SCHEMA,
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
        (TRANSACTIONS_YAML_FILE, f"{CH_SCHEMA}.{TRANSACTIONS_TABLE}"),
        (CUSTOMERS_YAML_FILE, f"{CH_SCHEMA}.{CUSTOMERS_TABLE}"),
        (COUNTRIES_YAML_FILE, f"{CH_SCHEMA}.{COUNTRIES_TABLE}"),
    ]:
        with open(yaml_file) as f:
            var_name = os.path.basename(yaml_file).replace(".yaml", "_table")
            contents = Template(f.read()).substitute({var_name: table_name})
        with open(os.path.join(model_dir, os.path.basename(yaml_file)), "w") as f:
            f.write(contents)


@pytest.fixture(scope="module")
def ch_model_dir():
    d = tempfile.mkdtemp(prefix="ch_test_models_")
    _generate_sample_model_configs(d)
    yield d
    shutil.rmtree(d, ignore_errors=True)


@pytest.fixture(scope="module")
def ch_sample_data(ch_client):
    from metricflow.cli.tutorial import create_sample_data, remove_sample_tables

    remove_sample_tables(sql_client=ch_client, system_schema=CH_SCHEMA)
    created = create_sample_data(sql_client=ch_client, system_schema=CH_SCHEMA)
    assert created, "Failed to create sample data"
    yield
    remove_sample_tables(sql_client=ch_client, system_schema=CH_SCHEMA)


@pytest.mark.clickhouse
class TestClickHouseValidateConfigs:
    """Test the full validate-configs pipeline against a live ClickHouse database."""

    def test_health_checks_via_config(self, ch_client, ch_model_dir):
        handler = _build_ch_handler_with_models(ch_model_dir)
        sql_client = make_sql_client_from_config(handler)
        results = sql_client.health_checks(schema_name=CH_SCHEMA)
        for name, result in results.items():
            assert result["status"] == "SUCCESS", f"{name}: {result}"

    def test_model_build_from_config(self, ch_model_dir):
        from metricflow.engine.utils import model_build_result_from_config

        handler = _build_ch_handler_with_models(ch_model_dir)
        build_result = model_build_result_from_config(
            handler=handler, raise_issues_as_exceptions=False
        )
        assert not build_result.issues.has_blocking_issues, (
            f"Model build had blocking issues: {build_result.issues.summary()}"
        )

    def test_semantic_validation(self, ch_model_dir):
        from metricflow.engine.utils import model_build_result_from_config
        from metricflow.model.model_validator import ModelValidator

        handler = _build_ch_handler_with_models(ch_model_dir)
        build_result = model_build_result_from_config(
            handler=handler, raise_issues_as_exceptions=False
        )
        semantic_result = ModelValidator().validate_model(build_result.model)
        assert not semantic_result.issues.has_blocking_issues, (
            f"Semantic validation had blocking issues: {semantic_result.issues.summary()}"
        )

    def test_data_warehouse_validation(self, ch_client, ch_model_dir, ch_sample_data):
        from metricflow.engine.utils import model_build_result_from_config
        from metricflow.model.data_warehouse_model_validator import DataWarehouseModelValidator
        from metricflow.model.validations.validator_helpers import ModelValidationResults

        handler = _build_ch_handler_with_models(ch_model_dir)
        build_result = model_build_result_from_config(
            handler=handler, raise_issues_as_exceptions=False
        )
        model = build_result.model
        dw_validator = DataWarehouseModelValidator(
            sql_client=ch_client, system_schema=CH_SCHEMA
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


@pytest.mark.clickhouse
class TestClickHouseQueryMetrics:
    """Test the full query --metrics pipeline against a live ClickHouse database."""

    def test_build_engine_from_config(self, ch_client, ch_model_dir, ch_sample_data):
        from metricflow.engine.metricflow_engine import MetricFlowEngine

        handler = _build_ch_handler_with_models(ch_model_dir)
        engine = MetricFlowEngine.from_config(handler)
        assert engine is not None

    def test_list_metrics(self, ch_client, ch_model_dir, ch_sample_data):
        from metricflow.engine.metricflow_engine import MetricFlowEngine

        handler = _build_ch_handler_with_models(ch_model_dir)
        engine = MetricFlowEngine.from_config(handler)
        metrics = engine.list_metrics()
        metric_names = [m.name for m in metrics]
        assert "transactions" in metric_names

    def test_explain_query(self, ch_client, ch_model_dir, ch_sample_data):
        from metricflow.engine.metricflow_engine import MetricFlowEngine

        handler = _build_ch_handler_with_models(ch_model_dir)
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

    def test_query_transactions_metric(self, ch_client, ch_model_dir, ch_sample_data):
        from metricflow.engine.metricflow_engine import MetricFlowEngine

        handler = _build_ch_handler_with_models(ch_model_dir)
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

    def test_query_with_dimension_filter(self, ch_client, ch_model_dir, ch_sample_data):
        from metricflow.engine.metricflow_engine import MetricFlowEngine

        handler = _build_ch_handler_with_models(ch_model_dir)
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
