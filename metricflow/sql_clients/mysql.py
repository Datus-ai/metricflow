import logging
import textwrap
import time
from typing import ClassVar, Mapping, Optional, Sequence, Union, Callable

import pandas as pd
import sqlalchemy

from metricflow.dataflow.sql_table import SqlTable
from metricflow.protocols.sql_client import SqlEngine, SqlIsolationLevel
from metricflow.protocols.sql_client import SqlEngineAttributes
from metricflow.protocols.sql_request import SqlRequestTagSet, SqlJsonTag
from metricflow.sql.sql_bind_parameters import SqlBindParameters
from metricflow.sql.render.mysql import MySQLSqlQueryPlanRenderer
from metricflow.sql.render.sql_plan_renderer import SqlQueryPlanRenderer
from metricflow.sql_clients.async_request import SqlStatementCommentMetadata, CombinedSqlTags
from metricflow.sql_clients.common_client import SqlDialect, not_empty
from metricflow.sql_clients.sqlalchemy_dialect import SqlAlchemySqlClient

logger = logging.getLogger(__name__)


class MySQLEngineAttributes:
    """Engine-specific attributes for the MySQL query engine

    This is an implementation of the SqlEngineAttributes protocol for MySQL
    """

    sql_engine_type: ClassVar[SqlEngine] = SqlEngine.MYSQL

    # SQL Engine capabilities
    supported_isolation_levels: ClassVar[Sequence[SqlIsolationLevel]] = ()
    date_trunc_supported: ClassVar[bool] = False  # MySQL doesn't have DATE_TRUNC
    full_outer_joins_supported: ClassVar[bool] = False  # MySQL doesn't support FULL OUTER JOIN
    indexes_supported: ClassVar[bool] = True
    multi_threading_supported: ClassVar[bool] = True
    timestamp_type_supported: ClassVar[bool] = True
    timestamp_to_string_comparison_supported: ClassVar[bool] = True
    cancel_submitted_queries_supported: ClassVar[bool] = True
    continuous_percentile_aggregation_supported: ClassVar[bool] = True
    discrete_percentile_aggregation_supported: ClassVar[bool] = True
    approximate_continuous_percentile_aggregation_supported: ClassVar[bool] = False
    approximate_discrete_percentile_aggregation_supported: ClassVar[bool] = False

    # SQL Dialect replacement strings
    double_data_type_name: ClassVar[str] = "DOUBLE"
    timestamp_type_name: ClassVar[Optional[str]] = "TIMESTAMP"
    random_function_name: ClassVar[str] = "RAND"

    # MetricFlow attributes
    sql_query_plan_renderer: ClassVar[SqlQueryPlanRenderer] = MySQLSqlQueryPlanRenderer()


class MySQLSqlClient(SqlAlchemySqlClient):
    """Implements MySQL."""

    @staticmethod
    def from_connection_details(url: str, password: Optional[str]) -> SqlAlchemySqlClient:  # noqa: D
        parsed_url = sqlalchemy.engine.url.make_url(url)
        dialect = SqlDialect.MYSQL.value
        if parsed_url.drivername != dialect:
            raise ValueError(f"Expected dialect '{dialect}' in {url}")

        if password is None:
            raise ValueError(f"Password not supplied for {url}")

        return MySQLSqlClient(
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
                dialect=SqlDialect.MYSQL.value,
                driver="pymysql",
                port=port,
                database=database,
                username=username,
                password=password,
                host=host,
                query=query,
            )
        )

    @property
    def sql_engine_attributes(self) -> SqlEngineAttributes:
        """Collection of attributes and features specific to the MySQL SQL engine"""
        return MySQLEngineAttributes()

    def create_table_from_dataframe(  # noqa: D
        self, sql_table: SqlTable, df: pd.DataFrame, chunk_size: Optional[int] = None
    ) -> None:
        logger.info(f"Creating table '{sql_table.sql}' from a DataFrame with {df.shape[0]} row(s)")
        start_time = time.time()

        # Create table schema based on DataFrame dtypes
        column_definitions = []
        for col_name, dtype in df.dtypes.items():
            if dtype == "object":
                sql_type = "TEXT"
            elif dtype == "int64":
                sql_type = "BIGINT"
            elif dtype == "float64":
                sql_type = "DOUBLE"
            elif dtype == "bool":
                sql_type = "BOOLEAN"
            elif "datetime" in str(dtype):
                sql_type = "DATETIME"
            else:
                sql_type = "TEXT"
            column_definitions.append(f"`{col_name}` {sql_type}")

        # Create table
        create_table_sql = f"CREATE TABLE {sql_table.sql} ({', '.join(column_definitions)})"
        self.execute(create_table_sql)

        # Insert data in chunks
        chunk_size = chunk_size or 1000
        for start_idx in range(0, len(df), chunk_size):
            end_idx = min(start_idx + chunk_size, len(df))
            chunk_df = df.iloc[start_idx:end_idx]

            # Prepare values for INSERT statement
            values_list = []
            for _, row in chunk_df.iterrows():
                values = []
                for value in row:
                    if pd.isna(value):
                        values.append("NULL")
                    elif isinstance(value, str):
                        # Escape single quotes
                        escaped_value = value.replace("'", "''")
                        values.append(f"'{escaped_value}'")
                    elif isinstance(value, (int, float)):
                        values.append(str(value))
                    elif isinstance(value, bool):
                        values.append("1" if value else "0")
                    else:
                        values.append(f"'{str(value)}'")
                values_list.append(f"({', '.join(values)})")

            # Insert chunk
            if values_list:
                insert_sql = f"INSERT INTO {sql_table.sql} VALUES {', '.join(values_list)}"
                self.execute(insert_sql)

        logger.info(f"Created table '{sql_table.sql}' from a DataFrame in {time.time() - start_time:.2f}s")

    def _engine_specific_query_implementation(
        self,
        stmt: str,
        bind_params: SqlBindParameters,
        isolation_level: Optional[SqlIsolationLevel] = None,
        system_tags: SqlRequestTagSet = SqlRequestTagSet(),
        extra_tags: SqlJsonTag = SqlJsonTag(),
    ) -> pd.DataFrame:
        """Override to use SQLAlchemy connection for pandas compatibility."""
        with self._engine_connection(
            self._engine, isolation_level=isolation_level, system_tags=system_tags, extra_tags=extra_tags
        ) as conn:
            return pd.read_sql_query(sqlalchemy.text(stmt), conn, params=bind_params.param_dict)

    def cancel_submitted_queries(self) -> None:  # noqa: D
        for request_id in self.active_requests():
            target_tags = SqlRequestTagSet.create_from_request_id(request_id)
            self.cancel_request(lambda tags: target_tags.is_subset_of(tags.system_tags))

    def cancel_request(self, match_function: Callable[[CombinedSqlTags], bool]) -> int:  # noqa: D
        result = self.query(
            textwrap.dedent(
                """\
                SELECT ID AS query_id, INFO AS query_text
                FROM INFORMATION_SCHEMA.PROCESSLIST
                WHERE COMMAND != 'Sleep' AND INFO NOT LIKE '%PROCESSLIST%'
                ORDER BY TIME DESC;
                """
            )
        )

        num_cancelled_queries = 0

        for query_id, query_text in result.values:
            if query_text:  # INFO can be NULL
                parsed_tags = SqlStatementCommentMetadata.parse_tag_metadata_in_comments(query_text)

                # Check for a match where the query's tag
                if match_function(parsed_tags):
                    logger.info(f"Cancelling query ID: {query_id}")
                    self.execute(f"KILL {query_id};")
                    num_cancelled_queries += 1

        return num_cancelled_queries
