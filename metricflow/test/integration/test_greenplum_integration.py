"""End-to-end integration tests for the Greenplum pipeline.

Tests the full flow: config -> client creation -> SQL rendering -> query execution.
- Tests marked with no marker run without a database (mock-based).
- Tests marked with @pytest.mark.greenplum require a real Greenplum instance.
  Uses Docker env from datus-greenplum: localhost:15432, gpadmin/pivotal.
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
    DEFAULT_SCHEMA_MAPPING,
)
from metricflow.engine.metricflow_engine import MetricFlowQueryRequest
from metricflow.protocols.sql_client import SqlEngine
from metricflow.sql.render.greenplum import GreenplumSqlQueryPlanRenderer
from metricflow.sql_clients.common_client import SqlDialect
from metricflow.sql_clients.greenplum import GreenplumEngineAttributes, GreenplumSqlClient
from metricflow.sql_clients.sql_utils import make_sql_client_from_config


# ---------------------------------------------------------------------------
# Config layer tests (no database needed)
# ---------------------------------------------------------------------------


class TestGreenplumDictConfigHandler:
    """Tests that DictConfigHandler correctly handles greenplum config."""

    def test_dialect_mapping_contains_greenplum(self) -> None:
        assert "greenplum" in DIALECT_MAPPING
        assert DIALECT_MAPPING["greenplum"] == "greenplum"

    def test_default_schema_for_greenplum(self) -> None:
        assert "greenplum" in DEFAULT_SCHEMA_MAPPING
        assert DEFAULT_SCHEMA_MAPPING["greenplum"] == "public"

    def test_build_config_dict_greenplum(self) -> None:
        config = build_config_dict_from_db_params(
            db_type="greenplum",
            host="gp-master",
            port="5432",
            username="gpadmin",
            password="secret",
            database="testdb",
        )
        assert config[CONFIG_DWH_DIALECT] == "greenplum"
        assert config[CONFIG_DWH_HOST] == "gp-master"
        assert config[CONFIG_DWH_PORT] == "5432"
        assert config[CONFIG_DWH_USER] == "gpadmin"
        assert config[CONFIG_DWH_PASSWORD] == "secret"
        assert config[CONFIG_DWH_DB] == "testdb"
        assert config[CONFIG_DWH_SCHEMA] == "public"

    def test_build_config_dict_greenplum_custom_schema(self) -> None:
        config = build_config_dict_from_db_params(
            db_type="greenplum",
            host="gp-master",
            port="5432",
            username="gpadmin",
            password="secret",
            database="testdb",
            schema="analytics",
        )
        assert config[CONFIG_DWH_SCHEMA] == "analytics"

    def test_dict_config_handler_get_value(self) -> None:
        config_dict = build_config_dict_from_db_params(
            db_type="greenplum",
            host="gp-master",
            port="5432",
            username="gpadmin",
            password="secret",
            database="testdb",
        )
        handler = DictConfigHandler(config_dict)
        assert handler.get_value(CONFIG_DWH_DIALECT) == "greenplum"
        assert handler.get_value(CONFIG_DWH_HOST) == "gp-master"
        assert handler.get_value(CONFIG_DWH_DB) == "testdb"


class TestGreenplumClientFromConfig:
    """Tests that make_sql_client_from_config creates GreenplumSqlClient for greenplum dialect."""

    def test_make_sql_client_from_config_creates_greenplum_client(self) -> None:
        """Verify the factory dispatches to GreenplumSqlClient.from_connection_details."""
        config_dict = build_config_dict_from_db_params(
            db_type="greenplum",
            host="gp-master",
            port="5432",
            username="gpadmin",
            password="secret",
            database="testdb",
        )
        handler = DictConfigHandler(config_dict)

        with patch.object(
            GreenplumSqlClient, "from_connection_details", return_value=MagicMock()
        ) as mock_factory:
            make_sql_client_from_config(handler)
            mock_factory.assert_called_once()
            call_args = mock_factory.call_args
            url = call_args[0][0]
            password = call_args[0][1]
            assert "greenplum://" in url
            assert "gpadmin" in url
            assert "gp-master" in url
            assert "5432" in url
            assert "testdb" in url
            assert password == "secret"


class TestGreenplumSqlRendering:
    """Tests that the Greenplum renderer produces correct SQL for full query plans."""

    def test_renderer_renders_simple_select(self) -> None:
        """Verify the Greenplum plan renderer can render a basic SQL plan."""
        from metricflow.dataflow.sql_table import SqlTable
        from metricflow.sql.sql_exprs import SqlColumnReferenceExpression, SqlColumnReference
        from metricflow.sql.sql_plan import (
            SqlQueryPlan,
            SqlSelectColumn,
            SqlSelectStatementNode,
            SqlTableFromClauseNode,
        )

        select_node = SqlSelectStatementNode(
            description="Greenplum simple query",
            select_columns=(
                SqlSelectColumn(
                    expr=SqlColumnReferenceExpression(SqlColumnReference("a", "revenue")),
                    column_alias="revenue",
                ),
            ),
            from_source=SqlTableFromClauseNode(sql_table=SqlTable(schema_name="public", table_name="sales")),
            from_source_alias="a",
            joins_descs=(),
            where=None,
            group_bys=(),
            order_bys=(),
        )
        plan = SqlQueryPlan(plan_id="gp_test", render_node=select_node)
        renderer = GreenplumSqlQueryPlanRenderer()
        rendered = renderer.render_sql_query_plan(plan)
        assert "SELECT" in rendered.sql
        assert "a.revenue" in rendered.sql
        assert "public.sales" in rendered.sql

    def test_renderer_renders_uuid_with_random(self) -> None:
        """Verify UUID rendering in a full query plan uses RANDOM concat."""
        from metricflow.dataflow.sql_table import SqlTable
        from metricflow.sql.sql_exprs import SqlGenerateUuidExpression
        from metricflow.sql.sql_plan import (
            SqlQueryPlan,
            SqlSelectColumn,
            SqlSelectStatementNode,
            SqlTableFromClauseNode,
        )

        select_node = SqlSelectStatementNode(
            description="Greenplum UUID query",
            select_columns=(
                SqlSelectColumn(
                    expr=SqlGenerateUuidExpression(),
                    column_alias="uuid",
                ),
            ),
            from_source=SqlTableFromClauseNode(sql_table=SqlTable(schema_name="public", table_name="t")),
            from_source_alias="a",
            joins_descs=(),
            where=None,
            group_bys=(),
            order_bys=(),
        )
        plan = SqlQueryPlan(plan_id="gp_uuid", render_node=select_node)
        renderer = GreenplumSqlQueryPlanRenderer()
        rendered = renderer.render_sql_query_plan(plan)
        assert "RANDOM()" in rendered.sql
        assert "CONCAT" in rendered.sql
        assert "GEN_RANDOM_UUID" not in rendered.sql

    def test_engine_attributes_renderer_is_greenplum(self) -> None:
        """Verify GreenplumEngineAttributes uses GreenplumSqlQueryPlanRenderer."""
        renderer = GreenplumEngineAttributes.sql_query_plan_renderer
        assert isinstance(renderer, GreenplumSqlQueryPlanRenderer)


class TestGreenplumCliSetupDialect:
    """Tests that the CLI setup command includes greenplum in dialect choices."""

    def test_greenplum_in_cli_dialect_map(self) -> None:
        from metricflow.cli.utils import MF_GREENPLUM_KEYS
        assert any(
            k.key == CONFIG_DWH_DIALECT and k.value == SqlDialect.GREENPLUM.value
            for k in MF_GREENPLUM_KEYS
        )


# ---------------------------------------------------------------------------
# Live database tests (require a real Greenplum instance)
# Uses Docker environment from datus-greenplum: localhost:15432, gpadmin/pivotal
# ---------------------------------------------------------------------------

GP_URL = "greenplum://gpadmin@localhost:15432/test"
GP_PASSWORD = "pivotal"


def _make_gp_client():
    """Create a GreenplumSqlClient connected to the Docker instance."""
    try:
        client = GreenplumSqlClient.from_connection_details(url=GP_URL, password=GP_PASSWORD)
        client.query("SELECT 1")
        return client
    except Exception:
        return None


@pytest.fixture(scope="module")
def gp_client():
    """Module-scoped fixture for a live Greenplum client."""
    client = _make_gp_client()
    if client is None:
        pytest.skip("Greenplum Docker not available at localhost:15432")
    yield client
    client.close()


@pytest.mark.greenplum
class TestGreenplumLiveDatabase:
    """Integration tests that require a real Greenplum database.

    Uses the Docker environment from datus-greenplum (docker-compose up -d).
    """

    def test_health_checks(self, gp_client):
        """Verify health checks pass against a live Greenplum database."""
        result = gp_client.health_checks(schema_name="public")
        assert isinstance(result, dict)
        for check_name, check_result in result.items():
            assert check_result["status"] == "SUCCESS", f"{check_name} failed: {check_result}"

    def test_simple_query(self, gp_client):
        """Execute SELECT 1 against Greenplum."""
        df = gp_client.query("SELECT 1 AS val")
        assert len(df) == 1
        assert df.columns.tolist() == ["val"]

    def test_create_table_and_query(self, gp_client):
        """Create a table, insert data, and query it back."""
        from metricflow.dataflow.sql_table import SqlTable
        from metricflow.object_utils import random_id

        table = SqlTable(schema_name="public", table_name=f"gp_test_{random_id()}")
        try:
            gp_client.create_table_as_select(table, "SELECT 42 AS answer, 'hello' AS greeting")
            df = gp_client.query(f"SELECT * FROM {table.sql}")
            assert len(df) == 1
            assert set(df.columns) == {"answer", "greeting"}
        finally:
            gp_client.execute(f"DROP TABLE IF EXISTS {table.sql}")

    def test_validate_configs_with_greenplum(self, gp_client):
        """Verify that validate-configs style checks work with Greenplum."""
        attrs = gp_client.sql_engine_attributes
        assert attrs.sql_engine_type == SqlEngine.GREENPLUM
        assert attrs.date_trunc_supported is True
        assert attrs.full_outer_joins_supported is True
        assert isinstance(attrs.sql_query_plan_renderer, GreenplumSqlQueryPlanRenderer)

    def test_query_with_date_trunc(self, gp_client):
        """Verify DATE_TRUNC works on Greenplum (common in metric queries)."""
        df = gp_client.query(
            "SELECT DATE_TRUNC('month', CAST('2024-03-15' AS TIMESTAMP)) AS metric_time"
        )
        assert len(df) == 1

    def test_query_with_percentile(self, gp_client):
        """Verify PERCENTILE_CONT works on Greenplum."""
        df = gp_client.query(
            "SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY v) AS median "
            "FROM (SELECT generate_series(1, 10) AS v) t"
        )
        assert len(df) == 1

    def test_rendered_sql_executes(self, gp_client):
        """Render a SQL plan via GreenplumSqlQueryPlanRenderer and execute it on GP."""
        from metricflow.dataflow.sql_table import SqlTable
        from metricflow.sql.sql_exprs import SqlColumnReferenceExpression, SqlColumnReference
        from metricflow.sql.sql_plan import (
            SqlQueryPlan,
            SqlSelectColumn,
            SqlSelectStatementNode,
            SqlTableFromClauseNode,
        )
        from metricflow.object_utils import random_id

        # Create a source table
        src = SqlTable(schema_name="public", table_name=f"gp_render_test_{random_id()}")
        try:
            gp_client.create_table_as_select(src, "SELECT 100 AS revenue, 'us' AS country")

            # Build and render a plan
            select_node = SqlSelectStatementNode(
                description="GP live render test",
                select_columns=(
                    SqlSelectColumn(
                        expr=SqlColumnReferenceExpression(SqlColumnReference("a", "revenue")),
                        column_alias="revenue",
                    ),
                ),
                from_source=SqlTableFromClauseNode(sql_table=src),
                from_source_alias="a",
                joins_descs=(),
                where=None,
                group_bys=(),
                order_bys=(),
            )
            plan = SqlQueryPlan(plan_id="gp_live", render_node=select_node)
            renderer = GreenplumSqlQueryPlanRenderer()
            rendered = renderer.render_sql_query_plan(plan)

            # Execute rendered SQL on real GP
            df = gp_client.query(rendered.sql)
            assert len(df) == 1
            assert df["revenue"].iloc[0] == 100
        finally:
            gp_client.execute(f"DROP TABLE IF EXISTS {src.sql}")

    def test_list_tables(self, gp_client):
        """Verify list_tables works on Greenplum (GP 4.x compat)."""
        from metricflow.dataflow.sql_table import SqlTable
        from metricflow.object_utils import random_id

        table = SqlTable(schema_name="public", table_name=f"gp_list_{random_id()}")
        try:
            gp_client.create_table_as_select(table, "SELECT 1 AS x")
            tables = gp_client.list_tables("public")
            assert table.table_name in tables
            assert gp_client.table_exists(table)
        finally:
            gp_client.execute(f"DROP TABLE IF EXISTS {table.sql}")


# ---------------------------------------------------------------------------
# Helper: build DictConfigHandler with model_path for GP Docker env
# ---------------------------------------------------------------------------

GP_SCHEMA = "public"


def _build_gp_handler_with_models(model_dir: str) -> DictConfigHandler:
    """Create a DictConfigHandler pointing at the GP Docker and a model dir."""
    config_dict = build_config_dict_from_db_params(
        db_type="greenplum",
        host="localhost",
        port="15432",
        username="gpadmin",
        password="pivotal",
        database="test",
        schema=GP_SCHEMA,
        model_path=model_dir,
    )
    return DictConfigHandler(config_dict)


def _generate_sample_model_configs(model_dir: str) -> None:
    """Write sample model YAML files into model_dir, bound to GP_SCHEMA."""
    from metricflow.cli.tutorial import (
        TRANSACTIONS_YAML_FILE,
        CUSTOMERS_YAML_FILE,
        COUNTRIES_YAML_FILE,
        TRANSACTIONS_TABLE,
        CUSTOMERS_TABLE,
        COUNTRIES_TABLE,
    )

    for yaml_file, table_name in [
        (TRANSACTIONS_YAML_FILE, f"{GP_SCHEMA}.{TRANSACTIONS_TABLE}"),
        (CUSTOMERS_YAML_FILE, f"{GP_SCHEMA}.{CUSTOMERS_TABLE}"),
        (COUNTRIES_YAML_FILE, f"{GP_SCHEMA}.{COUNTRIES_TABLE}"),
    ]:
        with open(yaml_file) as f:
            var_name = os.path.basename(yaml_file).replace(".yaml", "_table")
            contents = Template(f.read()).substitute({var_name: table_name})
        with open(os.path.join(model_dir, os.path.basename(yaml_file)), "w") as f:
            f.write(contents)


@pytest.fixture(scope="module")
def gp_model_dir():
    """Module-scoped temp dir containing sample model YAML files."""
    d = tempfile.mkdtemp(prefix="gp_test_models_")
    _generate_sample_model_configs(d)
    yield d
    shutil.rmtree(d, ignore_errors=True)


@pytest.fixture(scope="module")
def gp_sample_data(gp_client):
    """Seed sample data into GP and clean up after all tests."""
    from metricflow.cli.tutorial import create_sample_data, remove_sample_tables

    remove_sample_tables(sql_client=gp_client, system_schema=GP_SCHEMA)
    created = create_sample_data(sql_client=gp_client, system_schema=GP_SCHEMA)
    assert created, "Failed to create sample data"
    yield
    remove_sample_tables(sql_client=gp_client, system_schema=GP_SCHEMA)


# ---------------------------------------------------------------------------
# validate-configs flow tests
# ---------------------------------------------------------------------------


@pytest.mark.greenplum
class TestGreenplumValidateConfigs:
    """Test the full validate-configs pipeline against a live Greenplum database.

    Mirrors what `mf validate-configs` does:
    1. Health checks (DW connectivity)
    2. Model build (parse YAML)
    3. Semantic validation
    4. Data warehouse validation (check tables/columns exist)
    """

    def test_health_checks_via_config(self, gp_client, gp_model_dir):
        """validate-configs step 1: DW health checks via DictConfigHandler."""
        handler = _build_gp_handler_with_models(gp_model_dir)
        sql_client = make_sql_client_from_config(handler)
        results = sql_client.health_checks(schema_name=GP_SCHEMA)
        for name, result in results.items():
            assert result["status"] == "SUCCESS", f"{name}: {result}"

    def test_model_build_from_config(self, gp_model_dir):
        """validate-configs step 2: parse model YAML without errors."""
        from metricflow.engine.utils import model_build_result_from_config

        handler = _build_gp_handler_with_models(gp_model_dir)
        build_result = model_build_result_from_config(
            handler=handler, raise_issues_as_exceptions=False
        )
        assert not build_result.issues.has_blocking_issues, (
            f"Model build had blocking issues: {build_result.issues.summary()}"
        )

    def test_semantic_validation(self, gp_model_dir):
        """validate-configs step 3: semantic validation passes."""
        from metricflow.engine.utils import model_build_result_from_config
        from metricflow.model.model_validator import ModelValidator

        handler = _build_gp_handler_with_models(gp_model_dir)
        build_result = model_build_result_from_config(
            handler=handler, raise_issues_as_exceptions=False
        )
        semantic_result = ModelValidator().validate_model(build_result.model)
        assert not semantic_result.issues.has_blocking_issues, (
            f"Semantic validation had blocking issues: {semantic_result.issues.summary()}"
        )

    def test_data_warehouse_validation(self, gp_client, gp_model_dir, gp_sample_data):
        """validate-configs step 4: DW validation checks tables/columns exist."""
        from metricflow.engine.utils import model_build_result_from_config
        from metricflow.model.data_warehouse_model_validator import DataWarehouseModelValidator
        from metricflow.model.validations.validator_helpers import ModelValidationResults

        handler = _build_gp_handler_with_models(gp_model_dir)
        build_result = model_build_result_from_config(
            handler=handler, raise_issues_as_exceptions=False
        )
        model = build_result.model
        dw_validator = DataWarehouseModelValidator(
            sql_client=gp_client, system_schema=GP_SCHEMA
        )
        # Run all DW validations (same as _data_warehouse_validations_runner in CLI)
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


# ---------------------------------------------------------------------------
# query --metrics flow tests
# ---------------------------------------------------------------------------


@pytest.mark.greenplum
class TestGreenplumQueryMetrics:
    """Test the full query --metrics pipeline against a live Greenplum database.

    Mirrors what `mf query --metrics <metric> --dimensions metric_time` does:
    1. Build MetricFlowEngine from config
    2. List metrics
    3. Explain (render SQL)
    4. Query (execute and return results)
    """

    def test_build_engine_from_config(self, gp_client, gp_model_dir, gp_sample_data):
        """Build MetricFlowEngine using DictConfigHandler pointing at GP."""
        from metricflow.engine.metricflow_engine import MetricFlowEngine

        handler = _build_gp_handler_with_models(gp_model_dir)
        engine = MetricFlowEngine.from_config(handler)
        assert engine is not None

    def test_list_metrics(self, gp_client, gp_model_dir, gp_sample_data):
        """mf list-metrics: verify metrics are discoverable."""
        from metricflow.engine.metricflow_engine import MetricFlowEngine

        handler = _build_gp_handler_with_models(gp_model_dir)
        engine = MetricFlowEngine.from_config(handler)
        metrics = engine.list_metrics()
        metric_names = [m.name for m in metrics]
        assert "transactions" in metric_names
        assert "transaction_amount_usd" in metric_names

    def test_explain_query(self, gp_client, gp_model_dir, gp_sample_data):
        """mf query --explain: verify SQL generation for Greenplum."""
        from metricflow.engine.metricflow_engine import MetricFlowEngine

        handler = _build_gp_handler_with_models(gp_model_dir)
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
        assert "transactions" in sql.lower() or "1" in sql

    def test_query_transactions_metric(self, gp_client, gp_model_dir, gp_sample_data):
        """mf query --metrics transactions --dimensions metric_time: execute on GP."""
        from metricflow.engine.metricflow_engine import MetricFlowEngine

        handler = _build_gp_handler_with_models(gp_model_dir)
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

    def test_query_transaction_amount_metric(self, gp_client, gp_model_dir, gp_sample_data):
        """mf query --metrics transaction_amount_usd --dimensions metric_time."""
        from metricflow.engine.metricflow_engine import MetricFlowEngine

        handler = _build_gp_handler_with_models(gp_model_dir)
        engine = MetricFlowEngine.from_config(handler)

        mf_request = MetricFlowQueryRequest.create_with_random_request_id(
            metric_names=["transaction_amount_usd"],
            group_by_names=["metric_time"],
            order_by_names=["metric_time"],
            limit=5,
        )
        result = engine.query(mf_request=mf_request)
        df = result.result_df
        assert df is not None
        assert len(df) > 0
        assert "transaction_amount_usd" in df.columns

    def test_query_with_dimension_filter(self, gp_client, gp_model_dir, gp_sample_data):
        """mf query --metrics transactions --dimensions metric_time --where."""
        from metricflow.engine.metricflow_engine import MetricFlowEngine

        handler = _build_gp_handler_with_models(gp_model_dir)
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
