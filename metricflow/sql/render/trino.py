from metricflow.object_utils import assert_values_exhausted
from metricflow.sql.render.expr_renderer import (
    DefaultSqlExpressionRenderer,
    SqlExpressionRenderer,
    SqlExpressionRenderResult,
)
from metricflow.sql.render.sql_plan_renderer import DefaultSqlQueryPlanRenderer
from metricflow.sql.sql_bind_parameters import SqlBindParameters
from metricflow.sql.sql_exprs import (
    SqlGenerateUuidExpression,
    SqlPercentileExpression,
    SqlPercentileFunctionType,
    SqlTimeDeltaExpression,
)
from metricflow.time.time_granularity import TimeGranularity


class TrinoSqlExpressionRenderer(DefaultSqlExpressionRenderer):
    """Expression renderer for the Trino engine.

    Trino supports standard DATE_TRUNC and CAST TO TIMESTAMP, so we only override
    UUID generation, time delta, and percentile expressions.
    """

    def visit_time_delta_expr(self, node: SqlTimeDeltaExpression) -> SqlExpressionRenderResult:  # noqa: D
        arg_rendered = node.arg.accept(self)
        if node.grain_to_date:
            return SqlExpressionRenderResult(
                sql=f"DATE_TRUNC('{node.granularity.value}', {arg_rendered.sql})",
                execution_parameters=arg_rendered.execution_parameters,
            )

        count = node.count
        granularity = node.granularity
        if granularity == TimeGranularity.QUARTER:
            granularity = TimeGranularity.MONTH
            count *= 3
        return SqlExpressionRenderResult(
            sql=f"date_add('{granularity.value}', -{count}, {arg_rendered.sql})",
            execution_parameters=arg_rendered.execution_parameters,
        )

    def visit_generate_uuid_expr(self, node: SqlGenerateUuidExpression) -> SqlExpressionRenderResult:  # noqa: D
        return SqlExpressionRenderResult(
            sql="CAST(UUID() AS VARCHAR)",
            execution_parameters=SqlBindParameters(),
        )

    def visit_percentile_expr(self, node: SqlPercentileExpression) -> SqlExpressionRenderResult:  # noqa: D
        """Trino only supports approx_percentile; exact percentiles are unsupported."""
        arg_rendered = self.render_sql_expr(node.order_by_arg)
        params = arg_rendered.execution_parameters
        percentile = node.percentile_args.percentile

        if node.percentile_args.function_type is SqlPercentileFunctionType.CONTINUOUS:
            raise RuntimeError(
                "Trino does not support exact continuous percentile aggregation. "
                "Use approximate_continuous instead."
            )
        elif node.percentile_args.function_type is SqlPercentileFunctionType.DISCRETE:
            raise RuntimeError(
                "Trino does not support exact discrete percentile aggregation. "
                "Use approximate_discrete instead."
            )
        elif node.percentile_args.function_type is SqlPercentileFunctionType.APPROXIMATE_CONTINUOUS:
            sql = f"approx_percentile({arg_rendered.sql}, {percentile})"
        elif node.percentile_args.function_type is SqlPercentileFunctionType.APPROXIMATE_DISCRETE:
            sql = f"approx_percentile({arg_rendered.sql}, {percentile})"
        else:
            assert_values_exhausted(node.percentile_args.function_type)

        return SqlExpressionRenderResult(
            sql=sql,
            execution_parameters=params,
        )


class TrinoSqlQueryPlanRenderer(DefaultSqlQueryPlanRenderer):
    """Plan renderer for the Trino engine."""

    EXPR_RENDERER = TrinoSqlExpressionRenderer()

    @property
    def expr_renderer(self) -> SqlExpressionRenderer:  # noqa :D
        return self.EXPR_RENDERER
