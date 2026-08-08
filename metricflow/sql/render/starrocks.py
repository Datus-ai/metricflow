from metricflow.object_utils import assert_values_exhausted
from metricflow.sql.render.expr_renderer import SqlExpressionRenderer, SqlExpressionRenderResult
from metricflow.sql.render.mysql import MySQLSqlExpressionRenderer
from metricflow.sql.render.sql_plan_renderer import DefaultSqlQueryPlanRenderer
from metricflow.sql.sql_exprs import (
    SqlDateTruncExpression,
    SqlPercentileExpression,
    SqlPercentileFunctionType,
    SqlTimeDeltaExpression,
)


class StarRocksSqlExpressionRenderer(MySQLSqlExpressionRenderer):
    """Expression renderer for the StarRocks engine.

    StarRocks is highly compatible with MySQL, but has a native DATE_TRUNC implementation.
    """

    def visit_date_trunc_expr(self, node: SqlDateTruncExpression) -> SqlExpressionRenderResult:  # noqa: D
        arg_rendered = self.render_sql_expr(node.arg)
        return SqlExpressionRenderResult(
            sql=f"DATE_TRUNC('{node.time_granularity.value}', {arg_rendered.sql})",
            execution_parameters=arg_rendered.execution_parameters,
        )

    def visit_time_delta_expr(self, node: SqlTimeDeltaExpression) -> SqlExpressionRenderResult:  # noqa: D
        if node.grain_to_date:
            arg_rendered = self.render_sql_expr(node.arg)
            return SqlExpressionRenderResult(
                sql=f"DATE_TRUNC('{node.granularity.value}', {arg_rendered.sql})",
                execution_parameters=arg_rendered.execution_parameters,
            )
        return super().visit_time_delta_expr(node)

    def visit_percentile_expr(self, node: SqlPercentileExpression) -> SqlExpressionRenderResult:  # noqa: D
        arg_rendered = self.render_sql_expr(node.order_by_arg)
        percentile = node.percentile_args.percentile

        if node.percentile_args.function_type is SqlPercentileFunctionType.CONTINUOUS:
            function_name = "PERCENTILE_CONT"
        elif node.percentile_args.function_type is SqlPercentileFunctionType.DISCRETE:
            function_name = "PERCENTILE_DISC"
        elif node.percentile_args.function_type is SqlPercentileFunctionType.APPROXIMATE_CONTINUOUS:
            function_name = "PERCENTILE_APPROX"
        elif node.percentile_args.function_type is SqlPercentileFunctionType.APPROXIMATE_DISCRETE:
            raise RuntimeError("Approximate discrete percentile aggregation is not supported for StarRocks.")
        else:
            assert_values_exhausted(node.percentile_args.function_type)

        return SqlExpressionRenderResult(
            sql=f"{function_name}({arg_rendered.sql}, {percentile})",
            execution_parameters=arg_rendered.execution_parameters,
        )


class StarRocksSqlQueryPlanRenderer(DefaultSqlQueryPlanRenderer):
    """Plan renderer for the StarRocks engine."""

    EXPR_RENDERER = StarRocksSqlExpressionRenderer()

    @property
    def expr_renderer(self) -> SqlExpressionRenderer:  # noqa :D
        return self.EXPR_RENDERER
