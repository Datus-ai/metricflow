from metricflow.sql.render.expr_renderer import (
    SqlExpressionRenderer,
    SqlExpressionRenderResult,
)
from metricflow.sql.render.postgres import PostgresSqlExpressionRenderer
from metricflow.sql.render.sql_plan_renderer import DefaultSqlQueryPlanRenderer
from metricflow.sql.sql_bind_parameters import SqlBindParameters
from metricflow.sql.sql_exprs import SqlGenerateUuidExpression


class GreenplumSqlExpressionRenderer(PostgresSqlExpressionRenderer):
    """Expression renderer for the Greenplum engine.

    Greenplum is based on PostgreSQL, so we inherit most behavior from Postgres.
    Only UUID generation needs to be overridden since Greenplum lacks GEN_RANDOM_UUID().
    """

    def visit_generate_uuid_expr(self, node: SqlGenerateUuidExpression) -> SqlExpressionRenderResult:  # noqa: D
        return SqlExpressionRenderResult(
            sql="CONCAT(CAST(RANDOM()*100000000 AS INT)::VARCHAR,CAST(RANDOM()*100000000 AS INT)::VARCHAR)",
            execution_parameters=SqlBindParameters(),
        )


class GreenplumSqlQueryPlanRenderer(DefaultSqlQueryPlanRenderer):
    """Plan renderer for the Greenplum engine."""

    EXPR_RENDERER = GreenplumSqlExpressionRenderer()

    @property
    def expr_renderer(self) -> SqlExpressionRenderer:  # noqa :D
        return self.EXPR_RENDERER
