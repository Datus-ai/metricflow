from __future__ import annotations

import datetime
from dataclasses import dataclass, field

from metricflow.constraints.time_constraint import TimeRangeConstraint
from metricflow.engine.time_source import ServerTimeSource
from metricflow.protocols.sql_client import SqlEngine
from metricflow.sql.sql_plan import SqlSelectQueryFromClauseNode
from metricflow.time.time_granularity import TimeGranularity
from metricflow.time.time_source import TimeSource


class TimeSpineGenerationError(RuntimeError):
    """Base error raised when a dynamic time spine cannot be generated."""


class TimeSpineDialectNotSupportedError(TimeSpineGenerationError):
    """Raised when a SQL engine has no read-only time-spine implementation."""


class TimeSpineRangeTooLargeError(TimeSpineGenerationError):
    """Raised when a requested time spine exceeds a safe engine-specific size."""


@dataclass(frozen=True)
class TimeSpineSource:
    """Defines a read-only SQL source that generates timestamps for time-based metrics."""

    sql_engine: SqlEngine
    time_column_name: str = "ds"
    time_column_granularity: TimeGranularity = TimeGranularity.DAY
    max_points: int = 100_000
    default_range_days: int = 365
    time_source: TimeSource = field(default_factory=ServerTimeSource, repr=False, compare=False)

    def make_source(self, time_range_constraint: TimeRangeConstraint | None) -> SqlSelectQueryFromClauseNode:
        """Build a bounded, read-only SQL query that returns the requested time spine."""

        if time_range_constraint is None:
            end_time = self.time_source.get_time()
            time_range_constraint = TimeRangeConstraint(
                start_time=end_time - datetime.timedelta(days=self.default_range_days),
                end_time=end_time,
            )

        if self.time_column_granularity != TimeGranularity.DAY:
            raise TimeSpineGenerationError(
                f"Dynamic time spines with granularity {self.time_column_granularity} are not supported."
            )

        start_time = _start_of_day(time_range_constraint.start_time)
        end_time = _start_of_day(time_range_constraint.end_time)
        point_count = (end_time - start_time).days + 1
        if point_count <= 0:
            raise TimeSpineGenerationError("The time-spine start time must be less than or equal to the end time.")
        if point_count > self.max_points:
            raise TimeSpineRangeTooLargeError(
                f"Dynamic time-spine generation for {self.sql_engine.value} would produce {point_count} points, "
                f"which exceeds the safety limit of {self.max_points}. Use a smaller time range. "
                "No database objects were created."
            )

        select_query = _make_time_spine_select_query(
            sql_engine=self.sql_engine,
            start_time=start_time,
            end_time=end_time,
            point_count=point_count,
            column_name=self.time_column_name,
        )
        return SqlSelectQueryFromClauseNode(select_query=select_query)


def _timestamp_text(value: datetime.datetime) -> str:
    """Format an already parsed datetime for an internal SQL timestamp literal."""

    return value.strftime("%Y-%m-%d %H:%M:%S")


def _start_of_day(value: datetime.datetime) -> datetime.datetime:
    """Normalize a time-spine boundary to the midnight anchor used by a daily spine."""

    return value.replace(hour=0, minute=0, second=0, microsecond=0)


def _make_time_spine_select_query(
    sql_engine: SqlEngine,
    start_time: datetime.datetime,
    end_time: datetime.datetime,
    point_count: int,
    column_name: str,
) -> str:
    """Return dialect-specific SQL for an inclusive daily timestamp series."""

    start = _timestamp_text(start_time)
    end = _timestamp_text(end_time)

    if sql_engine in (SqlEngine.POSTGRES, SqlEngine.GREENPLUM, SqlEngine.DUCKDB):
        return f"""\
SELECT {column_name}
FROM GENERATE_SERIES(
  CAST('{start}' AS TIMESTAMP),
  CAST('{end}' AS TIMESTAMP),
  INTERVAL '1 day'
) AS time_spine({column_name})"""

    if sql_engine is SqlEngine.BIGQUERY:
        return f"""\
SELECT CAST({column_name} AS DATETIME) AS {column_name}
FROM UNNEST(
  GENERATE_TIMESTAMP_ARRAY(TIMESTAMP('{start}'), TIMESTAMP('{end}'), INTERVAL 1 DAY)
) AS {column_name}"""

    if sql_engine is SqlEngine.SNOWFLAKE:
        return f"""\
SELECT DATEADD(day, ROW_NUMBER() OVER (ORDER BY SEQ4()) - 1, TO_TIMESTAMP_NTZ('{start}')) AS {column_name}
FROM TABLE(GENERATOR(ROWCOUNT => {point_count}))"""

    if sql_engine is SqlEngine.DATABRICKS:
        return f"""\
SELECT EXPLODE(
  SEQUENCE(TIMESTAMP '{start}', TIMESTAMP '{end}', INTERVAL 1 DAY)
) AS {column_name}"""

    if sql_engine is SqlEngine.TRINO:
        return f"""\
SELECT {column_name}
FROM UNNEST(
  SEQUENCE(TIMESTAMP '{start}', TIMESTAMP '{end}', INTERVAL '1' DAY)
) AS time_spine({column_name})"""

    if sql_engine is SqlEngine.CLICKHOUSE:
        return f"""\
SELECT addDays(toDateTime('{start}'), number) AS {column_name}
FROM numbers({point_count})"""

    if sql_engine is SqlEngine.STARROCKS:
        return f"""\
SELECT TIMESTAMPADD(DAY, generate_series, CAST('{start}' AS DATETIME)) AS {column_name}
FROM TABLE(generate_series(0, {point_count - 1}))"""

    if sql_engine is SqlEngine.MYSQL:
        if point_count > 1_000:
            raise TimeSpineRangeTooLargeError(
                f"Dynamic time-spine generation for MySQL would produce {point_count} points, which exceeds the "
                "safe recursive CTE limit of 1000. Use a smaller time range. No database objects were created."
            )
        return f"""\
WITH RECURSIVE time_spine({column_name}) AS (
  SELECT CAST('{start}' AS DATETIME)
  UNION ALL
  SELECT DATE_ADD({column_name}, INTERVAL 1 DAY)
  FROM time_spine
  WHERE {column_name} < CAST('{end}' AS DATETIME)
)
SELECT {column_name}
FROM time_spine"""

    if sql_engine is SqlEngine.SQLITE:
        if point_count > 1_000:
            raise TimeSpineRangeTooLargeError(
                f"Dynamic time-spine generation for SQLite would produce {point_count} points, which exceeds the "
                "safe recursive CTE limit of 1000. Use a smaller time range. No database objects were created."
            )
        return f"""\
WITH RECURSIVE time_spine({column_name}) AS (
  SELECT datetime('{start}')
  UNION ALL
  SELECT datetime({column_name}, '+1 day')
  FROM time_spine
  WHERE {column_name} < datetime('{end}')
)
SELECT {column_name}
FROM time_spine"""

    raise TimeSpineDialectNotSupportedError(
        f"Dynamic time-spine generation is not supported for {sql_engine.value}. "
        "This metric requires a continuous time series because it uses a cumulative window or time offset. "
        "No database objects were created."
    )
