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
    SqlGenerateUuidExpression,
    SqlPercentileExpression,
    SqlPercentileFunctionType,
    SqlTimeDeltaExpression,
)
from metricflow.time.time_granularity import TimeGranularity


class MySQLSqlExpressionRenderer(DefaultSqlExpressionRenderer):
    """Expression renderer for the MySQL engine."""

    @property
    def double_data_type(self) -> str:
        """Custom double data type for the MySQL engine"""
        return "DOUBLE"

    def visit_cast_to_timestamp_expr(self, node: SqlCastToTimestampExpression) -> SqlExpressionRenderResult:  # noqa: D
        """Render CAST to timestamp for MySQL/StarRocks using DATETIME type"""
        arg_rendered = self.render_sql_expr(node.arg)
        return SqlExpressionRenderResult(
            sql=f"CAST({arg_rendered.sql} AS DATETIME)",
            execution_parameters=arg_rendered.execution_parameters,
        )

    def visit_time_delta_expr(self, node: SqlTimeDeltaExpression) -> SqlExpressionRenderResult:  # noqa: D
        arg_rendered = node.arg.accept(self)
        if node.grain_to_date:
            # MySQL doesn't have DATE_TRUNC, use DATE_FORMAT for similar functionality
            if node.granularity == TimeGranularity.DAY:
                return SqlExpressionRenderResult(
                    sql=f"DATE({arg_rendered.sql})",
                    execution_parameters=arg_rendered.execution_parameters,
                )
            elif node.granularity == TimeGranularity.WEEK:
                return SqlExpressionRenderResult(
                    sql=f"DATE_SUB({arg_rendered.sql}, INTERVAL WEEKDAY({arg_rendered.sql}) DAY)",
                    execution_parameters=arg_rendered.execution_parameters,
                )
            elif node.granularity == TimeGranularity.MONTH:
                return SqlExpressionRenderResult(
                    sql=f"DATE_FORMAT({arg_rendered.sql}, '%Y-%m-01')",
                    execution_parameters=arg_rendered.execution_parameters,
                )
            elif node.granularity == TimeGranularity.QUARTER:
                return SqlExpressionRenderResult(
                    sql=f"DATE_FORMAT({arg_rendered.sql}, '%Y-%m-01')",
                    execution_parameters=arg_rendered.execution_parameters,
                )
            elif node.granularity == TimeGranularity.YEAR:
                return SqlExpressionRenderResult(
                    sql=f"DATE_FORMAT({arg_rendered.sql}, '%Y-01-01')",
                    execution_parameters=arg_rendered.execution_parameters,
                )
            else:
                return SqlExpressionRenderResult(
                    sql=f"DATE({arg_rendered.sql})",
                    execution_parameters=arg_rendered.execution_parameters,
                )

        count = node.count
        granularity = node.granularity
        if granularity == TimeGranularity.QUARTER:
            granularity = TimeGranularity.MONTH
            count *= 3

        # Use MySQL's DATE_SUB function with INTERVAL
        return SqlExpressionRenderResult(
            sql=f"DATE_SUB({arg_rendered.sql}, INTERVAL {count} {granularity.value})",
            execution_parameters=arg_rendered.execution_parameters,
        )

    def visit_generate_uuid_expr(self, node: SqlGenerateUuidExpression) -> SqlExpressionRenderResult:  # noqa: D
        return SqlExpressionRenderResult(
            sql="UUID()",
            execution_parameters=SqlBindParameters(),
        )

    def visit_percentile_expr(self, node: SqlPercentileExpression) -> SqlExpressionRenderResult:
        """Render a percentile expression for MySQL."""
        arg_rendered = self.render_sql_expr(node.order_by_arg)
        params = arg_rendered.execution_parameters
        percentile = node.percentile_args.percentile

        if node.percentile_args.function_type is SqlPercentileFunctionType.CONTINUOUS:
            # MySQL doesn't have PERCENTILE_CONT, use approximation
            function_str = f"PERCENTILE_CONT({percentile})"
        elif node.percentile_args.function_type is SqlPercentileFunctionType.DISCRETE:
            # MySQL doesn't have PERCENTILE_DISC, use approximation
            function_str = f"PERCENTILE_DISC({percentile})"
        elif node.percentile_args.function_type is SqlPercentileFunctionType.APPROXIMATE_CONTINUOUS:
            function_str = f"PERCENTILE_CONT({percentile})"
        elif node.percentile_args.function_type is SqlPercentileFunctionType.APPROXIMATE_DISCRETE:
            function_str = f"PERCENTILE_DISC({percentile})"
        else:
            assert_values_exhausted(node.percentile_args.function_type)

        return SqlExpressionRenderResult(
            sql=f"{function_str} WITHIN GROUP (ORDER BY ({arg_rendered.sql}))",
            execution_parameters=params,
        )


class MySQLSqlQueryPlanRenderer(DefaultSqlQueryPlanRenderer):
    """Plan renderer for the MySQL engine."""

    EXPR_RENDERER = MySQLSqlExpressionRenderer()

    @property
    def expr_renderer(self) -> SqlExpressionRenderer:  # noqa :D
        return self.EXPR_RENDERER
