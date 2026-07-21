from metricflow.sql.render.expr_renderer import SqlExpressionRenderer
from metricflow.sql.render.mysql import MySQLSqlExpressionRenderer
from metricflow.sql.render.sql_plan_renderer import DefaultSqlQueryPlanRenderer


class DorisSqlExpressionRenderer(MySQLSqlExpressionRenderer):
    """Expression renderer for Apache Doris.

    Doris supports the MySQL-compatible expressions emitted by MetricFlow's
    MySQL renderer, including its date and time rendering behavior.
    """

    pass


class DorisSqlQueryPlanRenderer(DefaultSqlQueryPlanRenderer):
    """Plan renderer for Apache Doris."""

    EXPR_RENDERER = DorisSqlExpressionRenderer()

    @property
    def expr_renderer(self) -> SqlExpressionRenderer:  # noqa: D
        return self.EXPR_RENDERER
