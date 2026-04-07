"""Unit tests for Greenplum SQL rendering.

Tests that the Greenplum renderer correctly inherits PostgreSQL behavior
and overrides only what's needed (UUID generation).
"""

import pytest

from metricflow.sql.render.greenplum import GreenplumSqlExpressionRenderer, GreenplumSqlQueryPlanRenderer
from metricflow.sql.render.postgres import PostgresSqlExpressionRenderer
from metricflow.sql.sql_bind_parameters import SqlBindParameters
from metricflow.sql.sql_exprs import (
    SqlCastToTimestampExpression,
    SqlColumnReference,
    SqlColumnReferenceExpression,
    SqlGenerateUuidExpression,
    SqlPercentileExpression,
    SqlPercentileExpressionArgument,
    SqlPercentileFunctionType,
    SqlStringLiteralExpression,
    SqlTimeDeltaExpression,
)
from metricflow.time.time_granularity import TimeGranularity


@pytest.fixture
def greenplum_renderer() -> GreenplumSqlExpressionRenderer:
    return GreenplumSqlExpressionRenderer()


@pytest.fixture
def postgres_renderer() -> PostgresSqlExpressionRenderer:
    return PostgresSqlExpressionRenderer()


class TestGreenplumRendererInheritance:
    """Tests that Greenplum renderer is a proper subclass of PostgreSQL renderer."""

    def test_inherits_from_postgres(self) -> None:
        assert issubclass(GreenplumSqlExpressionRenderer, PostgresSqlExpressionRenderer)

    def test_double_data_type(self, greenplum_renderer: GreenplumSqlExpressionRenderer) -> None:
        assert greenplum_renderer.double_data_type == "DOUBLE PRECISION"

    def test_plan_renderer_uses_greenplum_expr_renderer(self) -> None:
        plan_renderer = GreenplumSqlQueryPlanRenderer()
        assert isinstance(plan_renderer.expr_renderer, GreenplumSqlExpressionRenderer)


class TestGreenplumUuidRendering:
    """Tests that Greenplum UUID generation differs from PostgreSQL."""

    def test_uuid_uses_random_concat(self, greenplum_renderer: GreenplumSqlExpressionRenderer) -> None:
        result = greenplum_renderer.visit_generate_uuid_expr(SqlGenerateUuidExpression())
        assert "RANDOM()" in result.sql
        assert "CONCAT" in result.sql
        assert "GEN_RANDOM_UUID" not in result.sql

    def test_uuid_differs_from_postgres(
        self,
        greenplum_renderer: GreenplumSqlExpressionRenderer,
        postgres_renderer: PostgresSqlExpressionRenderer,
    ) -> None:
        gp_result = greenplum_renderer.visit_generate_uuid_expr(SqlGenerateUuidExpression())
        pg_result = postgres_renderer.visit_generate_uuid_expr(SqlGenerateUuidExpression())
        assert gp_result.sql != pg_result.sql
        assert "GEN_RANDOM_UUID" in pg_result.sql
        assert "CONCAT" in gp_result.sql

    def test_uuid_has_empty_bind_parameters(self, greenplum_renderer: GreenplumSqlExpressionRenderer) -> None:
        result = greenplum_renderer.visit_generate_uuid_expr(SqlGenerateUuidExpression())
        assert result.execution_parameters == SqlBindParameters()


class TestGreenplumTimeDeltaRendering:
    """Tests that Greenplum inherits PostgreSQL time delta rendering."""

    def test_time_delta_uses_make_interval(self, greenplum_renderer: GreenplumSqlExpressionRenderer) -> None:
        col_expr = SqlColumnReferenceExpression(SqlColumnReference("a", "ds"))
        node = SqlTimeDeltaExpression(arg=col_expr, count=1, granularity=TimeGranularity.DAY, grain_to_date=False)
        result = greenplum_renderer.visit_time_delta_expr(node)
        assert "MAKE_INTERVAL" in result.sql

    def test_time_delta_grain_to_date(self, greenplum_renderer: GreenplumSqlExpressionRenderer) -> None:
        col_expr = SqlColumnReferenceExpression(SqlColumnReference("a", "ds"))
        node = SqlTimeDeltaExpression(arg=col_expr, count=1, granularity=TimeGranularity.MONTH, grain_to_date=True)
        result = greenplum_renderer.visit_time_delta_expr(node)
        assert "DATE_TRUNC" in result.sql

    def test_time_delta_quarter_converts_to_months(self, greenplum_renderer: GreenplumSqlExpressionRenderer) -> None:
        col_expr = SqlColumnReferenceExpression(SqlColumnReference("a", "ds"))
        node = SqlTimeDeltaExpression(arg=col_expr, count=1, granularity=TimeGranularity.QUARTER, grain_to_date=False)
        result = greenplum_renderer.visit_time_delta_expr(node)
        assert "months => 3" in result.sql

    def test_time_delta_matches_postgres(
        self,
        greenplum_renderer: GreenplumSqlExpressionRenderer,
        postgres_renderer: PostgresSqlExpressionRenderer,
    ) -> None:
        col_expr = SqlColumnReferenceExpression(SqlColumnReference("a", "ds"))
        node = SqlTimeDeltaExpression(arg=col_expr, count=7, granularity=TimeGranularity.DAY, grain_to_date=False)
        gp_result = greenplum_renderer.visit_time_delta_expr(node)
        pg_result = postgres_renderer.visit_time_delta_expr(node)
        assert gp_result.sql == pg_result.sql


class TestGreenplumPercentileRendering:
    """Tests that Greenplum inherits PostgreSQL percentile rendering."""

    def test_continuous_percentile(self, greenplum_renderer: GreenplumSqlExpressionRenderer) -> None:
        node = SqlPercentileExpression(
            order_by_arg=SqlColumnReferenceExpression(SqlColumnReference("a", "col0")),
            percentile_args=SqlPercentileExpressionArgument(
                percentile=0.5, function_type=SqlPercentileFunctionType.CONTINUOUS
            ),
        )
        result = greenplum_renderer.visit_percentile_expr(node)
        assert "PERCENTILE_CONT(0.5)" in result.sql
        assert "WITHIN GROUP" in result.sql

    def test_discrete_percentile(self, greenplum_renderer: GreenplumSqlExpressionRenderer) -> None:
        node = SqlPercentileExpression(
            order_by_arg=SqlColumnReferenceExpression(SqlColumnReference("a", "col0")),
            percentile_args=SqlPercentileExpressionArgument(
                percentile=0.5, function_type=SqlPercentileFunctionType.DISCRETE
            ),
        )
        result = greenplum_renderer.visit_percentile_expr(node)
        assert "PERCENTILE_DISC(0.5)" in result.sql

    def test_approximate_continuous_raises(self, greenplum_renderer: GreenplumSqlExpressionRenderer) -> None:
        node = SqlPercentileExpression(
            order_by_arg=SqlColumnReferenceExpression(SqlColumnReference("a", "col0")),
            percentile_args=SqlPercentileExpressionArgument(
                percentile=0.5, function_type=SqlPercentileFunctionType.APPROXIMATE_CONTINUOUS
            ),
        )
        with pytest.raises(RuntimeError):
            greenplum_renderer.visit_percentile_expr(node)

    def test_approximate_discrete_raises(self, greenplum_renderer: GreenplumSqlExpressionRenderer) -> None:
        node = SqlPercentileExpression(
            order_by_arg=SqlColumnReferenceExpression(SqlColumnReference("a", "col0")),
            percentile_args=SqlPercentileExpressionArgument(
                percentile=0.5, function_type=SqlPercentileFunctionType.APPROXIMATE_DISCRETE
            ),
        )
        with pytest.raises(RuntimeError):
            greenplum_renderer.visit_percentile_expr(node)

    def test_percentile_matches_postgres(
        self,
        greenplum_renderer: GreenplumSqlExpressionRenderer,
        postgres_renderer: PostgresSqlExpressionRenderer,
    ) -> None:
        node = SqlPercentileExpression(
            order_by_arg=SqlColumnReferenceExpression(SqlColumnReference("a", "col0")),
            percentile_args=SqlPercentileExpressionArgument(
                percentile=0.75, function_type=SqlPercentileFunctionType.CONTINUOUS
            ),
        )
        gp_result = greenplum_renderer.visit_percentile_expr(node)
        pg_result = postgres_renderer.visit_percentile_expr(node)
        assert gp_result.sql == pg_result.sql


class TestGreenplumCastToTimestamp:
    """Tests that Greenplum inherits PostgreSQL cast-to-timestamp rendering."""

    def test_cast_to_timestamp(self, greenplum_renderer: GreenplumSqlExpressionRenderer) -> None:
        node = SqlCastToTimestampExpression(arg=SqlStringLiteralExpression(literal_value="2020-01-01"))
        result = greenplum_renderer.render_sql_expr(node)
        assert result.sql == "CAST('2020-01-01' AS TIMESTAMP)"
