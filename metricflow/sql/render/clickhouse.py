from metricflow.object_utils import assert_values_exhausted
from metricflow.sql.render.expr_renderer import (
    DefaultSqlExpressionRenderer,
    SqlExpressionRenderer,
    SqlExpressionRenderResult,
)
from metricflow.sql.render.sql_plan_renderer import DefaultSqlQueryPlanRenderer
from metricflow.sql.sql_bind_parameters import SqlBindParameters
from metricflow.sql.sql_exprs import (
    SqlCastToTimestampExpression,
    SqlDateTruncExpression,
    SqlGenerateUuidExpression,
    SqlPercentileExpression,
    SqlPercentileFunctionType,
    SqlTimeDeltaExpression,
)
from metricflow.time.time_granularity import TimeGranularity


class ClickHouseSqlExpressionRenderer(DefaultSqlExpressionRenderer):
    """Expression renderer for the ClickHouse engine."""

    @property
    def double_data_type(self) -> str:  # noqa: D
        return "Float64"

    def visit_cast_to_timestamp_expr(self, node: SqlCastToTimestampExpression) -> SqlExpressionRenderResult:  # noqa: D
        arg_rendered = self.render_sql_expr(node.arg)
        return SqlExpressionRenderResult(
            sql=f"toDateTime({arg_rendered.sql})",
            execution_parameters=arg_rendered.execution_parameters,
        )

    @staticmethod
    def _render_to_start_of(arg_sql: str, granularity: TimeGranularity) -> str:
        """Map a TimeGranularity to the corresponding ClickHouse toStartOf* call."""
        if granularity == TimeGranularity.WEEK:
            return f"toStartOfWeek({arg_sql}, 1)"  # 1 = Monday
        elif granularity == TimeGranularity.MONTH:
            return f"toStartOfMonth({arg_sql})"
        elif granularity == TimeGranularity.QUARTER:
            return f"toStartOfQuarter({arg_sql})"
        elif granularity == TimeGranularity.YEAR:
            return f"toStartOfYear({arg_sql})"
        else:
            # DAY and any unknown granularity fall back to toStartOfDay
            return f"toStartOfDay({arg_sql})"

    def visit_date_trunc_expr(self, node: SqlDateTruncExpression) -> SqlExpressionRenderResult:  # noqa: D
        """ClickHouse uses toStartOf* functions instead of DATE_TRUNC."""
        arg_rendered = self.render_sql_expr(node.arg)
        return SqlExpressionRenderResult(
            sql=self._render_to_start_of(arg_rendered.sql, node.time_granularity),
            execution_parameters=arg_rendered.execution_parameters,
        )

    def visit_time_delta_expr(self, node: SqlTimeDeltaExpression) -> SqlExpressionRenderResult:  # noqa: D
        arg_rendered = node.arg.accept(self)
        if node.grain_to_date:
            return SqlExpressionRenderResult(
                sql=self._render_to_start_of(arg_rendered.sql, node.granularity),
                execution_parameters=arg_rendered.execution_parameters,
            )

        count = node.count
        granularity = node.granularity
        if granularity == TimeGranularity.QUARTER:
            granularity = TimeGranularity.MONTH
            count *= 3
        return SqlExpressionRenderResult(
            sql=f"date_sub({granularity.value}, {count}, {arg_rendered.sql})",
            execution_parameters=arg_rendered.execution_parameters,
        )

    def visit_generate_uuid_expr(self, node: SqlGenerateUuidExpression) -> SqlExpressionRenderResult:  # noqa: D
        return SqlExpressionRenderResult(
            sql="generateUUIDv4()",
            execution_parameters=SqlBindParameters(),
        )

    def visit_percentile_expr(self, node: SqlPercentileExpression) -> SqlExpressionRenderResult:  # noqa: D
        """ClickHouse uses quantile(p)(col) and quantileExact(p)(col)."""
        arg_rendered = self.render_sql_expr(node.order_by_arg)
        params = arg_rendered.execution_parameters
        percentile = node.percentile_args.percentile

        if node.percentile_args.function_type is SqlPercentileFunctionType.CONTINUOUS:
            sql = f"quantile({percentile})({arg_rendered.sql})"
        elif node.percentile_args.function_type is SqlPercentileFunctionType.DISCRETE:
            sql = f"quantileExact({percentile})({arg_rendered.sql})"
        elif node.percentile_args.function_type is SqlPercentileFunctionType.APPROXIMATE_CONTINUOUS:
            sql = f"quantile({percentile})({arg_rendered.sql})"
        elif node.percentile_args.function_type is SqlPercentileFunctionType.APPROXIMATE_DISCRETE:
            sql = f"quantileExact({percentile})({arg_rendered.sql})"
        else:
            assert_values_exhausted(node.percentile_args.function_type)

        return SqlExpressionRenderResult(
            sql=sql,
            execution_parameters=params,
        )


class ClickHouseSqlQueryPlanRenderer(DefaultSqlQueryPlanRenderer):
    """Plan renderer for the ClickHouse engine."""

    EXPR_RENDERER = ClickHouseSqlExpressionRenderer()

    @property
    def expr_renderer(self) -> SqlExpressionRenderer:  # noqa :D
        return self.EXPR_RENDERER
