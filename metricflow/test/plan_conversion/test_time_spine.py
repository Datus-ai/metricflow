import datetime

import pytest
from pandas import DataFrame

from metricflow.constraints.time_constraint import TimeRangeConstraint
from metricflow.plan_conversion.time_spine import (
    TimeSpineDialectNotSupportedError,
    TimeSpineRangeTooLargeError,
    TimeSpineSource,
)
from metricflow.protocols.sql_client import SqlClient, SqlEngine
from metricflow.time.time_constants import ISO8601_PYTHON_FORMAT, ISO8601_PYTHON_TS_FORMAT


def test_dynamic_time_spine_date_range(sql_client: SqlClient, time_spine_source: TimeSpineSource) -> None:  # noqa: D
    start_time = datetime.datetime(2020, 1, 1)
    end_time = datetime.datetime(2020, 1, 10)
    source = time_spine_source.make_source(TimeRangeConstraint(start_time=start_time, end_time=end_time))

    range_df: DataFrame = sql_client.query(
        f"""\
        SELECT
            COUNT(*)
            , MIN({time_spine_source.time_column_name})
            , MAX({time_spine_source.time_column_name})
        FROM (
{source.select_query}
        ) AS time_spine
        """,
    )
    assert range_df.shape == (1, 3), f"Expected 1 row with 3 columns in range dataframe, got {range_df}"
    row = tuple(range_df.squeeze())
    point_count, date_range = row[0], row[1:]
    assert point_count == 10

    if sql_client.sql_engine_attributes.timestamp_type_supported:
        assert tuple(value.strftime(ISO8601_PYTHON_TS_FORMAT) for value in date_range) == (
            start_time.strftime(ISO8601_PYTHON_TS_FORMAT),
            end_time.strftime(ISO8601_PYTHON_TS_FORMAT),
        )
    else:
        assert date_range == (
            start_time.strftime(ISO8601_PYTHON_FORMAT),
            end_time.strftime(ISO8601_PYTHON_FORMAT),
        )


@pytest.mark.parametrize("sql_engine", (SqlEngine.REDSHIFT,))
def test_unsupported_dynamic_time_spine_dialect(sql_engine: SqlEngine) -> None:
    source = TimeSpineSource(sql_engine=sql_engine)

    with pytest.raises(TimeSpineDialectNotSupportedError, match="No database objects were created"):
        source.make_source(
            TimeRangeConstraint(
                start_time=datetime.datetime(2020, 1, 1),
                end_time=datetime.datetime(2020, 1, 2),
            )
        )


@pytest.mark.parametrize(
    ("sql_engine", "expected_sql"),
    (
        (SqlEngine.POSTGRES, "GENERATE_SERIES"),
        (SqlEngine.GREENPLUM, "GENERATE_SERIES"),
        (SqlEngine.DUCKDB, "GENERATE_SERIES"),
        (SqlEngine.BIGQUERY, "GENERATE_TIMESTAMP_ARRAY"),
        (SqlEngine.SNOWFLAKE, "GENERATOR(ROWCOUNT => 2)"),
        (SqlEngine.DATABRICKS, "EXPLODE"),
        (SqlEngine.TRINO, "UNNEST"),
        (SqlEngine.CLICKHOUSE, "addDays"),
        (SqlEngine.STARROCKS, "TABLE(generate_series(0, 1))"),
        (SqlEngine.MYSQL, "WITH RECURSIVE"),
        (SqlEngine.SQLITE, "WITH RECURSIVE"),
    ),
)
def test_dynamic_time_spine_sql_by_dialect(sql_engine: SqlEngine, expected_sql: str) -> None:
    source = TimeSpineSource(sql_engine=sql_engine)
    generated = source.make_source(
        TimeRangeConstraint(
            start_time=datetime.datetime(2020, 1, 1),
            end_time=datetime.datetime(2020, 1, 2),
        )
    )

    assert expected_sql in generated.select_query


def test_mysql_dynamic_time_spine_rejects_unsafe_recursive_range() -> None:
    source = TimeSpineSource(sql_engine=SqlEngine.MYSQL)

    with pytest.raises(TimeSpineRangeTooLargeError, match="safe recursive CTE limit"):
        source.make_source(
            TimeRangeConstraint(
                start_time=datetime.datetime(2020, 1, 1),
                end_time=datetime.datetime(2022, 12, 31),
            )
        )


def test_dynamic_time_spine_normalizes_boundaries_to_midnight() -> None:
    source = TimeSpineSource(sql_engine=SqlEngine.DUCKDB)

    generated = source.make_source(
        TimeRangeConstraint(
            start_time=datetime.datetime(2020, 1, 1, 16, 14, 38),
            end_time=datetime.datetime(2020, 1, 3, 8, 30, 12),
        )
    )

    assert "2020-01-01 00:00:00" in generated.select_query
    assert "2020-01-03 00:00:00" in generated.select_query
    assert "16:14:38" not in generated.select_query
    assert "08:30:12" not in generated.select_query
