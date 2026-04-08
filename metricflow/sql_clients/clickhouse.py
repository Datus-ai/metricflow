import logging
import time
from typing import ClassVar, Mapping, Optional, Sequence, Union

import pandas as pd
import sqlalchemy

from metricflow.dataflow.sql_table import SqlTable
from metricflow.protocols.sql_client import SqlEngine, SqlIsolationLevel
from metricflow.protocols.sql_client import SqlEngineAttributes
from metricflow.protocols.sql_request import SqlRequestTagSet, SqlJsonTag
from metricflow.sql.sql_bind_parameters import SqlBindParameters
from metricflow.sql.render.clickhouse import ClickHouseSqlQueryPlanRenderer
from metricflow.sql.render.sql_plan_renderer import SqlQueryPlanRenderer
from metricflow.sql_clients.common_client import SqlDialect, not_empty
from metricflow.sql_clients.sqlalchemy_dialect import SqlAlchemySqlClient

logger = logging.getLogger(__name__)


class ClickHouseEngineAttributes:
    """Engine-specific attributes for the ClickHouse query engine."""

    sql_engine_type: ClassVar[SqlEngine] = SqlEngine.CLICKHOUSE

    # SQL Engine capabilities
    supported_isolation_levels: ClassVar[Sequence[SqlIsolationLevel]] = ()
    date_trunc_supported: ClassVar[bool] = False  # Uses toStartOf* functions
    full_outer_joins_supported: ClassVar[bool] = False
    indexes_supported: ClassVar[bool] = False
    multi_threading_supported: ClassVar[bool] = True
    timestamp_type_supported: ClassVar[bool] = True
    timestamp_to_string_comparison_supported: ClassVar[bool] = True
    cancel_submitted_queries_supported: ClassVar[bool] = False
    continuous_percentile_aggregation_supported: ClassVar[bool] = True
    discrete_percentile_aggregation_supported: ClassVar[bool] = True
    approximate_continuous_percentile_aggregation_supported: ClassVar[bool] = True
    approximate_discrete_percentile_aggregation_supported: ClassVar[bool] = True

    # SQL Dialect replacement strings
    double_data_type_name: ClassVar[str] = "Float64"
    timestamp_type_name: ClassVar[Optional[str]] = "DateTime"
    random_function_name: ClassVar[str] = "rand"

    # MetricFlow attributes
    sql_query_plan_renderer: ClassVar[SqlQueryPlanRenderer] = ClickHouseSqlQueryPlanRenderer()


class ClickHouseSqlClient(SqlAlchemySqlClient):
    """Implements ClickHouse.

    Uses clickhouse+http driver via clickhouse-sqlalchemy / clickhouse-connect.
    """

    @staticmethod
    def from_connection_details(url: str, password: Optional[str]) -> SqlAlchemySqlClient:  # noqa: D
        parsed_url = sqlalchemy.engine.url.make_url(url)
        dialect = SqlDialect.CLICKHOUSE.value
        if parsed_url.drivername != dialect:
            raise ValueError(f"Expected dialect '{dialect}' in {url}")

        if password is None:
            password = ""

        return ClickHouseSqlClient(
            host=not_empty(parsed_url.host, "host", url),
            port=not_empty(parsed_url.port, "port", url),
            username=not_empty(parsed_url.username, "username", url),
            password=password,
            database=not_empty(parsed_url.database, "database", url),
            query=parsed_url.query,
        )

    def __init__(  # noqa: D
        self,
        port: int,
        database: str,
        username: str,
        password: str,
        host: str,
        query: Optional[Mapping[str, Union[str, Sequence[str]]]] = None,
    ) -> None:
        super().__init__(
            engine=self.create_engine(
                dialect=SqlDialect.CLICKHOUSE.value,
                driver="http",
                port=port,
                database=database,
                username=username,
                password=password,
                host=host,
                query=query,
            )
        )

    @property
    def sql_engine_attributes(self) -> SqlEngineAttributes:  # noqa: D
        return ClickHouseEngineAttributes()

    def create_table_as_select(
        self,
        sql_table: SqlTable,
        select_query: str,
        sql_bind_parameters: SqlBindParameters = SqlBindParameters(),
    ) -> None:  # noqa: D
        # ClickHouse requires ENGINE clause for table creation
        self.execute(
            f"CREATE TABLE {sql_table.sql} ENGINE = MergeTree() ORDER BY tuple() AS {select_query}",
            sql_bind_parameters=sql_bind_parameters,
        )

    def create_table_from_dataframe(  # noqa: D
        self, sql_table: SqlTable, df: pd.DataFrame, chunk_size: Optional[int] = None
    ) -> None:
        logger.info(f"Creating table '{sql_table.sql}' from a DataFrame with {df.shape[0]} row(s)")
        start_time = time.time()

        column_definitions = []
        for col_name, dtype in df.dtypes.items():
            if dtype == "object":
                sql_type = "String"
            elif dtype == "int64":
                sql_type = "Int64"
            elif dtype == "float64":
                sql_type = "Float64"
            elif dtype == "bool":
                sql_type = "UInt8"
            elif "datetime" in str(dtype):
                sql_type = "DateTime"
            else:
                sql_type = "String"
            escaped_col_name = col_name.replace("`", "``")
            column_definitions.append(f"`{escaped_col_name}` Nullable({sql_type})")

        create_table_sql = (
            f"CREATE TABLE {sql_table.sql} ({', '.join(column_definitions)}) " f"ENGINE = MergeTree() ORDER BY tuple()"
        )
        self.execute(create_table_sql)

        if chunk_size is None:
            chunk_size = 1000
        elif chunk_size <= 0:
            raise ValueError("chunk_size must be a positive integer")
        for start_idx in range(0, len(df), chunk_size):
            end_idx = min(start_idx + chunk_size, len(df))
            chunk_df = df.iloc[start_idx:end_idx]

            values_list = []
            for _, row in chunk_df.iterrows():
                values = []
                for value in row:
                    if pd.isna(value):
                        values.append("NULL")
                    elif isinstance(value, str):
                        escaped_value = value.replace("'", "''").replace("\\", "\\\\")
                        values.append(f"'{escaped_value}'")
                    elif isinstance(value, bool):
                        values.append("1" if value else "0")
                    elif isinstance(value, (int, float)):
                        values.append(str(value))
                    else:
                        escaped_value = str(value).replace("'", "''").replace("\\", "\\\\")
                        values.append(f"'{escaped_value}'")
                values_list.append(f"({', '.join(values)})")

            if values_list:
                insert_sql = f"INSERT INTO {sql_table.sql} VALUES {', '.join(values_list)}"
                self.execute(insert_sql)

        logger.info(f"Created table '{sql_table.sql}' from a DataFrame in {time.time() - start_time:.2f}s")

    def _engine_specific_query_implementation(
        self,
        stmt: str,
        bind_params: SqlBindParameters,
        isolation_level: Optional[SqlIsolationLevel] = None,
        system_tags: Optional[SqlRequestTagSet] = None,
        extra_tags: Optional[SqlJsonTag] = None,
    ) -> pd.DataFrame:
        if system_tags is None:
            system_tags = SqlRequestTagSet()
        if extra_tags is None:
            extra_tags = SqlJsonTag()
        with self._engine_connection(
            self._engine, isolation_level=isolation_level, system_tags=system_tags, extra_tags=extra_tags
        ) as conn:
            return pd.read_sql_query(sqlalchemy.text(stmt), conn, params=bind_params.param_dict)

    def list_tables(self, schema_name: str) -> Sequence[str]:  # noqa: D
        escaped_name = schema_name.replace("`", "``")
        df = self.query(f"SHOW TABLES FROM `{escaped_name}`")
        if df.empty:
            return []
        return list(df.iloc[:, 0])

    def create_schema(self, schema_name: str) -> None:  # noqa: D
        escaped_name = schema_name.replace("`", "``")
        self.execute(f"CREATE DATABASE IF NOT EXISTS `{escaped_name}`")

    def drop_schema(self, schema_name: str, cascade: bool = True) -> None:  # noqa: D
        escaped_name = schema_name.replace("`", "``")
        if not cascade:
            # Check if database has tables before dropping
            try:
                df = self.query(f"SHOW TABLES FROM `{escaped_name}`")
                if not df.empty:
                    raise RuntimeError(f"Cannot drop database `{schema_name}` without cascade: it contains tables")
            except Exception as e:
                if "Cannot drop" in str(e):
                    raise
                # Database doesn't exist or can't be queried — safe to proceed
        self.execute(f"DROP DATABASE IF EXISTS `{escaped_name}`")

    def cancel_submitted_queries(self) -> None:  # noqa: D
        pass  # ClickHouse does not support query cancellation through this interface
