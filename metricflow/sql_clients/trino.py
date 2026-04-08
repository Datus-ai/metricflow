import logging
import time
from typing import ClassVar, Optional, Sequence

import pandas as pd
import sqlalchemy

from metricflow.dataflow.sql_table import SqlTable
from metricflow.protocols.sql_client import SqlEngine, SqlIsolationLevel
from metricflow.protocols.sql_client import SqlEngineAttributes
from metricflow.protocols.sql_request import SqlRequestTagSet, SqlJsonTag
from metricflow.sql.sql_bind_parameters import SqlBindParameters
from metricflow.sql.render.trino import TrinoSqlQueryPlanRenderer
from metricflow.sql.render.sql_plan_renderer import SqlQueryPlanRenderer
from metricflow.sql_clients.common_client import SqlDialect, not_empty
from metricflow.sql_clients.sqlalchemy_dialect import SqlAlchemySqlClient

logger = logging.getLogger(__name__)


class TrinoEngineAttributes:
    """Engine-specific attributes for the Trino query engine."""

    sql_engine_type: ClassVar[SqlEngine] = SqlEngine.TRINO

    # SQL Engine capabilities
    supported_isolation_levels: ClassVar[Sequence[SqlIsolationLevel]] = ()
    date_trunc_supported: ClassVar[bool] = True
    full_outer_joins_supported: ClassVar[bool] = True
    indexes_supported: ClassVar[bool] = False
    multi_threading_supported: ClassVar[bool] = True
    timestamp_type_supported: ClassVar[bool] = True
    timestamp_to_string_comparison_supported: ClassVar[bool] = True
    cancel_submitted_queries_supported: ClassVar[bool] = False
    continuous_percentile_aggregation_supported: ClassVar[bool] = False
    discrete_percentile_aggregation_supported: ClassVar[bool] = False
    approximate_continuous_percentile_aggregation_supported: ClassVar[bool] = True
    approximate_discrete_percentile_aggregation_supported: ClassVar[bool] = True

    # SQL Dialect replacement strings
    double_data_type_name: ClassVar[str] = "DOUBLE"
    timestamp_type_name: ClassVar[Optional[str]] = "TIMESTAMP"
    random_function_name: ClassVar[str] = "RANDOM"

    # MetricFlow attributes
    sql_query_plan_renderer: ClassVar[SqlQueryPlanRenderer] = TrinoSqlQueryPlanRenderer()


class TrinoSqlClient(SqlAlchemySqlClient):
    """Implements Trino.

    Uses the trino:// SQLAlchemy driver. URL format: trino://user@host:port/catalog
    """

    @staticmethod
    def from_connection_details(url: str, password: Optional[str]) -> SqlAlchemySqlClient:  # noqa: D
        parsed_url = sqlalchemy.engine.url.make_url(url)
        dialect = SqlDialect.TRINO.value
        if parsed_url.drivername != dialect:
            raise ValueError(f"Expected dialect '{dialect}' in {url}")

        # Trino memory connector doesn't require a password
        if password is None:
            password = ""

        return TrinoSqlClient(
            host=not_empty(parsed_url.host, "host", url),
            port=not_empty(parsed_url.port, "port", url),
            username=not_empty(parsed_url.username, "username", url),
            password=password,
            catalog=not_empty(parsed_url.database, "catalog", url),
        )

    def __init__(  # noqa: D
        self,
        port: int,
        catalog: str,
        username: str,
        host: str,
        password: str = "",
    ) -> None:
        # Trino URL format: trino://user:password@host:port/catalog
        connect_url = sqlalchemy.engine.url.URL.create(
            drivername="trino",
            username=username,
            password=password if password else None,
            host=host,
            port=port,
            database=catalog,
        )
        engine = sqlalchemy.create_engine(
            connect_url,
            pool_size=10,
            max_overflow=10,
            pool_pre_ping=True,
        )
        super().__init__(engine=engine)

    @property
    def sql_engine_attributes(self) -> SqlEngineAttributes:  # noqa: D
        return TrinoEngineAttributes()

    def create_table_from_dataframe(  # noqa: D
        self, sql_table: SqlTable, df: pd.DataFrame, chunk_size: Optional[int] = None
    ) -> None:
        logger.info(f"Creating table '{sql_table.sql}' from a DataFrame with {df.shape[0]} row(s)")
        start_time = time.time()

        column_types = []
        column_definitions = []
        for col_name, dtype in df.dtypes.items():
            if dtype == "object":
                sql_type = "VARCHAR"
            elif dtype == "int64":
                sql_type = "BIGINT"
            elif dtype == "float64":
                sql_type = "DOUBLE"
            elif dtype == "bool":
                sql_type = "BOOLEAN"
            elif "datetime" in str(dtype):
                sql_type = "TIMESTAMP"
            else:
                sql_type = "VARCHAR"
            column_types.append(sql_type)
            column_definitions.append(f"{col_name} {sql_type}")

        create_table_sql = f"CREATE TABLE {sql_table.sql} ({', '.join(column_definitions)})"
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
                for col_idx, value in enumerate(row):
                    declared_type = column_types[col_idx]
                    if pd.isna(value):
                        values.append("NULL")
                    elif isinstance(value, bool):
                        if declared_type == "BOOLEAN":
                            values.append("true" if value else "false")
                        else:
                            values.append(f"'{'true' if value else 'false'}'")
                    elif isinstance(value, str):
                        escaped_value = value.replace("'", "''")
                        values.append(f"'{escaped_value}'")
                    elif isinstance(value, (int, float)):
                        values.append(str(value))
                    elif hasattr(value, "strftime"):
                        if declared_type == "TIMESTAMP":
                            values.append(f"TIMESTAMP '{value.strftime('%Y-%m-%d %H:%M:%S')}'")
                        else:
                            values.append(f"'{value.strftime('%Y-%m-%d %H:%M:%S')}'")
                    else:
                        escaped_value = str(value).replace("'", "''")
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
        df = self.query(f"SHOW TABLES FROM {schema_name}")
        if df.empty:
            return []
        return list(df.iloc[:, 0])

    def create_schema(self, schema_name: str) -> None:  # noqa: D
        self.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")

    def cancel_submitted_queries(self) -> None:  # noqa: D
        pass  # Trino does not support query cancellation through this interface
