from metricflow.sql.render.expr_renderer import SqlExpressionRenderer
from metricflow.sql.render.mysql import MySQLSqlExpressionRenderer
from metricflow.sql.render.sql_plan_renderer import DefaultSqlQueryPlanRenderer


class StarRocksSqlExpressionRenderer(MySQLSqlExpressionRenderer):
    """Expression renderer for the StarRocks engine.

    StarRocks is highly compatible with MySQL, so we inherit all MySQL rendering behavior.
    The MySQL renderer already uses DAYOFWEEK() for StarRocks compatibility.
    """

    pass


class StarRocksSqlQueryPlanRenderer(DefaultSqlQueryPlanRenderer):
    """Plan renderer for the StarRocks engine."""

    EXPR_RENDERER = StarRocksSqlExpressionRenderer()

    @property
    def expr_renderer(self) -> SqlExpressionRenderer:  # noqa :D
        return self.EXPR_RENDERER
