"""End-to-end integration tests for the Trino pipeline.

Tests the full flow: config -> client creation -> SQL rendering -> query execution.
- Tests marked with no marker run without a database (mock-based).
- Tests marked with @pytest.mark.trino require a real Trino instance.
  Uses Docker env from datus-trino: localhost:8080, trino/(empty password), catalog=memory.
"""

import os
import shutil
import tempfile
from string import Template

import pytest
import sqlalchemy.exc
from unittest.mock import patch, MagicMock

from metricflow.configuration.constants import (
    CONFIG_DWH_DB,
    CONFIG_DWH_DIALECT,
    CONFIG_DWH_HOST,
    CONFIG_DWH_PASSWORD,
    CONFIG_DWH_PORT,
    CONFIG_DWH_SCHEMA,
    CONFIG_DWH_USER,
)
from metricflow.configuration.dict_config_handler import (
    DictConfigHandler,
    build_config_dict_from_db_params,
    DIALECT_MAPPING,
)
from metricflow.engine.metricflow_engine import MetricFlowQueryRequest
from metricflow.protocols.sql_client import SqlEngine
from metricflow.sql.render.trino import TrinoSqlQueryPlanRenderer
from metricflow.sql_clients.common_client import SqlDialect
from metricflow.sql_clients.trino import TrinoEngineAttributes, TrinoSqlClient
from metricflow.sql_clients.sql_utils import make_sql_client_from_config


# ---------------------------------------------------------------------------
# Config layer tests (no database needed)
# ---------------------------------------------------------------------------


class TestTrinoDictConfigHandler:
    """Tests that DictConfigHandler correctly handles trino config."""

    def test_dialect_mapping_contains_trino(self) -> None:
        assert "trino" in DIALECT_MAPPING
        assert DIALECT_MAPPING["trino"] == "trino"

    def test_build_config_dict_trino(self) -> None:
        config = build_config_dict_from_db_params(
            db_type="trino",
            host="trino-host",
            port="8090",
            username="trino",
            password="",
            database="memory",
        )
        assert config[CONFIG_DWH_DIALECT] == "trino"
        assert config[CONFIG_DWH_HOST] == "trino-host"
        assert config[CONFIG_DWH_PORT] == "8090"
        assert config[CONFIG_DWH_USER] == "trino"
        assert config[CONFIG_DWH_PASSWORD] == ""
        assert config[CONFIG_DWH_DB] == "memory"
        # Trino defaults to "default" schema
        assert config[CONFIG_DWH_SCHEMA] == "default"

    def test_build_config_dict_trino_custom_schema(self) -> None:
        config = build_config_dict_from_db_params(
            db_type="trino",
            host="trino-host",
            port="8090",
            username="trino",
            password="",
            database="memory",
            schema="analytics",
        )
        assert config[CONFIG_DWH_SCHEMA] == "analytics"

    def test_dict_config_handler_get_value(self) -> None:
        config_dict = build_config_dict_from_db_params(
            db_type="trino",
            host="trino-host",
            port="8090",
            username="trino",
            password="",
            database="memory",
        )
        handler = DictConfigHandler(config_dict)
        assert handler.get_value(CONFIG_DWH_DIALECT) == "trino"
        assert handler.get_value(CONFIG_DWH_HOST) == "trino-host"
        assert handler.get_value(CONFIG_DWH_DB) == "memory"


class TestTrinoClientFromConfig:
    """Tests that make_sql_client_from_config creates TrinoSqlClient for trino dialect."""

    def test_make_sql_client_from_config_creates_trino_client(self) -> None:
        config_dict = build_config_dict_from_db_params(
            db_type="trino",
            host="trino-host",
            port="8090",
            username="trino",
            password="",
            database="memory",
        )
        handler = DictConfigHandler(config_dict)

        with patch.object(TrinoSqlClient, "from_connection_details", return_value=MagicMock()) as mock_factory:
            make_sql_client_from_config(handler)
            mock_factory.assert_called_once()
            call_args = mock_factory.call_args
            url = call_args[0][0]
            password = call_args[0][1]
            assert "trino://" in url
            assert "trino" in url
            assert "trino-host" in url
            assert "8090" in url
            assert "memory" in url
            assert password == ""


class TestTrinoSqlRendering:
    """Tests that the Trino renderer produces correct SQL for full query plans."""

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
            description="Trino simple query",
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
        plan = SqlQueryPlan(plan_id="trino_test", render_node=select_node)
        renderer = TrinoSqlQueryPlanRenderer()
        rendered = renderer.render_sql_query_plan(plan)
        assert "SELECT" in rendered.sql
        assert "a.revenue" in rendered.sql
        assert "default.sales" in rendered.sql

    def test_renderer_renders_uuid_with_cast(self) -> None:
        from metricflow.dataflow.sql_table import SqlTable
        from metricflow.sql.sql_exprs import SqlGenerateUuidExpression
        from metricflow.sql.sql_plan import (
            SqlQueryPlan,
            SqlSelectColumn,
            SqlSelectStatementNode,
            SqlTableFromClauseNode,
        )

        select_node = SqlSelectStatementNode(
            description="Trino UUID query",
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
        plan = SqlQueryPlan(plan_id="trino_uuid", render_node=select_node)
        renderer = TrinoSqlQueryPlanRenderer()
        rendered = renderer.render_sql_query_plan(plan)
        assert "CAST(UUID() AS VARCHAR)" in rendered.sql

    def test_engine_attributes_renderer_is_trino(self) -> None:
        renderer = TrinoEngineAttributes.sql_query_plan_renderer
        assert isinstance(renderer, TrinoSqlQueryPlanRenderer)

    def test_trino_supports_full_outer_join(self) -> None:
        assert TrinoEngineAttributes.full_outer_joins_supported is True

    def test_trino_supports_date_trunc(self) -> None:
        assert TrinoEngineAttributes.date_trunc_supported is True

    def test_trino_only_supports_approximate_percentile(self) -> None:
        assert TrinoEngineAttributes.continuous_percentile_aggregation_supported is False
        assert TrinoEngineAttributes.discrete_percentile_aggregation_supported is False
        assert TrinoEngineAttributes.approximate_continuous_percentile_aggregation_supported is True
        assert TrinoEngineAttributes.approximate_discrete_percentile_aggregation_supported is True


class TestTrinoCliSetupDialect:
    """Tests that the CLI setup command includes trino in dialect choices."""

    def test_trino_in_cli_dialect_map(self) -> None:
        from metricflow.cli.utils import MF_TRINO_KEYS

        assert any(k.key == CONFIG_DWH_DIALECT and k.value == SqlDialect.TRINO.value for k in MF_TRINO_KEYS)


# ---------------------------------------------------------------------------
# Live database tests (require a real Trino instance)
# Uses Docker environment from datus-trino: localhost:8080, trino/(empty)
# ---------------------------------------------------------------------------

TRINO_URL = "trino://trino@localhost:8090/memory"
TRINO_PASSWORD = ""


def _make_trino_client():
    """Create a TrinoSqlClient connected to the Docker instance."""
    try:
        client = TrinoSqlClient.from_connection_details(url=TRINO_URL, password=TRINO_PASSWORD)
        client.query("SELECT 1")
        return client
    except (sqlalchemy.exc.OperationalError, ConnectionError, OSError):
        return None


@pytest.fixture(scope="module")
def trino_client():
    """Module-scoped fixture for a live Trino client."""
    client = _make_trino_client()
    if client is None:
        pytest.skip("Trino Docker not available at localhost:8090")
    yield client
    client.close()


@pytest.mark.trino
class TestTrinoLiveDatabase:
    """Integration tests that require a real Trino database."""

    def test_health_checks(self, trino_client):
        result = trino_client.health_checks(schema_name="default")
        assert isinstance(result, dict)
        for check_name, check_result in result.items():
            assert check_result["status"] == "SUCCESS", f"{check_name} failed: {check_result}"

    def test_simple_query(self, trino_client):
        df = trino_client.query("SELECT 1 AS val")
        assert len(df) == 1

    def test_create_table_and_query(self, trino_client):
        from metricflow.dataflow.sql_table import SqlTable
        from metricflow.object_utils import random_id

        # Trino memory connector: memory.default.<table>
        table = SqlTable(schema_name="default", table_name=f"trino_test_{random_id()}")
        try:
            trino_client.create_table_as_select(table, "SELECT 42 AS answer, 'hello' AS greeting")
            df = trino_client.query(f"SELECT * FROM {table.sql}")
            assert len(df) == 1
            assert set(df.columns) == {"answer", "greeting"}
        finally:
            trino_client.execute(f"DROP TABLE IF EXISTS {table.sql}")

    def test_validate_configs_with_trino(self, trino_client):
        attrs = trino_client.sql_engine_attributes
        assert attrs.sql_engine_type == SqlEngine.TRINO
        assert attrs.date_trunc_supported is True
        assert attrs.full_outer_joins_supported is True
        assert isinstance(attrs.sql_query_plan_renderer, TrinoSqlQueryPlanRenderer)

    def test_query_with_date_trunc(self, trino_client):
        df = trino_client.query("SELECT DATE_TRUNC('month', CAST('2024-03-15' AS TIMESTAMP)) AS metric_time")
        assert len(df) == 1

    def test_list_tables(self, trino_client):
        from metricflow.dataflow.sql_table import SqlTable
        from metricflow.object_utils import random_id

        table = SqlTable(schema_name="default", table_name=f"trino_list_{random_id()}")
        try:
            trino_client.create_table_as_select(table, "SELECT 1 AS x")
            tables = trino_client.list_tables("default")
            assert table.table_name in tables
        finally:
            trino_client.execute(f"DROP TABLE IF EXISTS {table.sql}")


# ---------------------------------------------------------------------------
# Helper: build DictConfigHandler with model_path for Trino Docker env
# ---------------------------------------------------------------------------

TRINO_SCHEMA = "default"


def _build_trino_handler_with_models(model_dir: str) -> DictConfigHandler:
    config_dict = build_config_dict_from_db_params(
        db_type="trino",
        host="localhost",
        port="8090",
        username="trino",
        password="",
        database="memory",
        schema=TRINO_SCHEMA,
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
        (TRANSACTIONS_YAML_FILE, f"{TRINO_SCHEMA}.{TRANSACTIONS_TABLE}"),
        (CUSTOMERS_YAML_FILE, f"{TRINO_SCHEMA}.{CUSTOMERS_TABLE}"),
        (COUNTRIES_YAML_FILE, f"{TRINO_SCHEMA}.{COUNTRIES_TABLE}"),
    ]:
        with open(yaml_file) as f:
            var_name = os.path.basename(yaml_file).replace(".yaml", "_table")
            contents = Template(f.read()).substitute({var_name: table_name})
        with open(os.path.join(model_dir, os.path.basename(yaml_file)), "w") as f:
            f.write(contents)


@pytest.fixture(scope="module")
def trino_model_dir():
    d = tempfile.mkdtemp(prefix="trino_test_models_")
    _generate_sample_model_configs(d)
    yield d
    shutil.rmtree(d, ignore_errors=True)


@pytest.fixture(scope="module")
def trino_sample_data(trino_client):
    from metricflow.cli.tutorial import create_sample_data, remove_sample_tables

    remove_sample_tables(sql_client=trino_client, system_schema=TRINO_SCHEMA)
    created = create_sample_data(sql_client=trino_client, system_schema=TRINO_SCHEMA)
    assert created, "Failed to create sample data"
    yield
    remove_sample_tables(sql_client=trino_client, system_schema=TRINO_SCHEMA)


@pytest.mark.trino
class TestTrinoValidateConfigs:
    """Test the full validate-configs pipeline against a live Trino database."""

    def test_health_checks_via_config(self, trino_client, trino_model_dir):
        handler = _build_trino_handler_with_models(trino_model_dir)
        sql_client = make_sql_client_from_config(handler)
        results = sql_client.health_checks(schema_name=TRINO_SCHEMA)
        for name, result in results.items():
            assert result["status"] == "SUCCESS", f"{name}: {result}"

    def test_model_build_from_config(self, trino_model_dir):
        from metricflow.engine.utils import model_build_result_from_config

        handler = _build_trino_handler_with_models(trino_model_dir)
        build_result = model_build_result_from_config(handler=handler, raise_issues_as_exceptions=False)
        assert (
            not build_result.issues.has_blocking_issues
        ), f"Model build had blocking issues: {build_result.issues.summary()}"

    def test_semantic_validation(self, trino_model_dir):
        from metricflow.engine.utils import model_build_result_from_config
        from metricflow.model.model_validator import ModelValidator

        handler = _build_trino_handler_with_models(trino_model_dir)
        build_result = model_build_result_from_config(handler=handler, raise_issues_as_exceptions=False)
        semantic_result = ModelValidator().validate_model(build_result.model)
        assert (
            not semantic_result.issues.has_blocking_issues
        ), f"Semantic validation had blocking issues: {semantic_result.issues.summary()}"

    def test_data_warehouse_validation(self, trino_client, trino_model_dir, trino_sample_data):
        from metricflow.engine.utils import model_build_result_from_config
        from metricflow.model.data_warehouse_model_validator import DataWarehouseModelValidator
        from metricflow.model.validations.validator_helpers import ModelValidationResults

        handler = _build_trino_handler_with_models(trino_model_dir)
        build_result = model_build_result_from_config(handler=handler, raise_issues_as_exceptions=False)
        model = build_result.model
        dw_validator = DataWarehouseModelValidator(sql_client=trino_client, system_schema=TRINO_SCHEMA)
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
        assert not merged.has_blocking_issues, f"DW validation had blocking issues: {merged.summary()}"


@pytest.mark.trino
class TestTrinoQueryMetrics:
    """Test the full query --metrics pipeline against a live Trino database."""

    def test_build_engine_from_config(self, trino_client, trino_model_dir, trino_sample_data):
        from metricflow.engine.metricflow_engine import MetricFlowEngine

        handler = _build_trino_handler_with_models(trino_model_dir)
        engine = MetricFlowEngine.from_config(handler)
        assert engine is not None

    def test_list_metrics(self, trino_client, trino_model_dir, trino_sample_data):
        from metricflow.engine.metricflow_engine import MetricFlowEngine

        handler = _build_trino_handler_with_models(trino_model_dir)
        engine = MetricFlowEngine.from_config(handler)
        metrics = engine.list_metrics()
        metric_names = [m.name for m in metrics]
        assert "transactions" in metric_names

    def test_explain_query(self, trino_client, trino_model_dir, trino_sample_data):
        from metricflow.engine.metricflow_engine import MetricFlowEngine

        handler = _build_trino_handler_with_models(trino_model_dir)
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

    def test_query_transactions_metric(self, trino_client, trino_model_dir, trino_sample_data):
        from metricflow.engine.metricflow_engine import MetricFlowEngine

        handler = _build_trino_handler_with_models(trino_model_dir)
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

    def test_query_with_dimension_filter(self, trino_client, trino_model_dir, trino_sample_data):
        from metricflow.engine.metricflow_engine import MetricFlowEngine

        handler = _build_trino_handler_with_models(trino_model_dir)
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
