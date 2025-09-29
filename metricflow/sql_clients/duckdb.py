import logging
import threading
import time
from typing import Callable, ClassVar, Optional, Sequence

import pandas as pd
import sqlalchemy
from sqlalchemy import inspect
from sqlalchemy.pool import StaticPool

try:
    from duckdb.typing import DuckDBPyType  # type: ignore[import-not-found]
except ImportError:  # pragma: no cover - duckdb is optional in some builds
    DuckDBPyType = None  # type: ignore[assignment]
else:
    if getattr(DuckDBPyType, "__hash__", None) is None:

        def _duckdb_py_type_hash(self: object) -> int:
            return hash(str(self))

        DuckDBPyType.__hash__ = _duckdb_py_type_hash  # type: ignore[attr-defined]

from metricflow.dataflow.sql_table import SqlTable
from metricflow.protocols.sql_client import SqlEngine, SqlIsolationLevel
from metricflow.protocols.sql_client import SqlEngineAttributes
from metricflow.protocols.sql_request import SqlRequestTagSet, SqlJsonTag
from metricflow.sql.render.duckdb_renderer import DuckDbSqlQueryPlanRenderer
from metricflow.sql.render.sql_plan_renderer import SqlQueryPlanRenderer
from metricflow.sql.sql_bind_parameters import SqlBindParameters
from metricflow.sql_clients.async_request import CombinedSqlTags
from metricflow.sql_clients.common_client import SqlDialect
from metricflow.sql_clients.sqlalchemy_dialect import SqlAlchemySqlClient

logger = logging.getLogger(__name__)


class DuckDbEngineAttributes:
    """Engine-specific attributes for the DuckDb query engine"""

    sql_engine_type: ClassVar[SqlEngine] = SqlEngine.DUCKDB

    # SQL Engine capabilities
    supported_isolation_levels: ClassVar[Sequence[SqlIsolationLevel]] = ()
    date_trunc_supported: ClassVar[bool] = True
    full_outer_joins_supported: ClassVar[bool] = True
    indexes_supported: ClassVar[bool] = True
    multi_threading_supported: ClassVar[bool] = True
    timestamp_type_supported: ClassVar[bool] = True
    timestamp_to_string_comparison_supported: ClassVar[bool] = True
    # Cancelling should be possible, but not yet implemented.
    cancel_submitted_queries_supported: ClassVar[bool] = False
    continuous_percentile_aggregation_supported: ClassVar[bool] = True
    discrete_percentile_aggregation_supported: ClassVar[bool] = True
    approximate_continuous_percentile_aggregation_supported: ClassVar[bool] = True
    approximate_discrete_percentile_aggregation_supported: ClassVar[bool] = False

    # SQL Dialect replacement strings
    double_data_type_name: ClassVar[str] = "DOUBLE"
    timestamp_type_name: ClassVar[Optional[str]] = "TIMESTAMP"
    random_function_name: ClassVar[str] = "RANDOM"

    # MetricFlow attributes
    sql_query_plan_renderer: ClassVar[SqlQueryPlanRenderer] = DuckDbSqlQueryPlanRenderer()


class DuckDbSqlClient(SqlAlchemySqlClient):
    """Implements DuckDB."""

    @staticmethod
    def from_connection_details(url: str, password: Optional[str] = None) -> SqlAlchemySqlClient:  # noqa: D
        parsed_url = sqlalchemy.engine.url.make_url(url)
        dialect = SqlDialect.DUCKDB.value
        if parsed_url.drivername != dialect:
            raise ValueError(f"Expected dialect '{dialect}' in {url}")

        if password:
            raise ValueError("Password should be empty")

        return DuckDbSqlClient(file_path=parsed_url.database)

    def __init__(self, file_path: Optional[str] = None) -> None:  # noqa: D
        # DuckDB is not designed with concurrency, but in can work in multi-threaded settings with
        # check_same_thread=False, StaticPool, and serializing of queries via a lock.
        self._concurrency_lock = threading.Lock()
        super().__init__(
            sqlalchemy.create_engine(
                f"duckdb:///{file_path if file_path else ':memory:'}",
                poolclass=StaticPool,
            )
        )

    @property
    def sql_engine_attributes(self) -> SqlEngineAttributes:
        """Collection of attributes and features specific to the Snowflake SQL engine"""
        return DuckDbEngineAttributes()

    def cancel_submitted_queries(self) -> None:  # noqa: D
        raise NotImplementedError

    def _engine_specific_query_implementation(
        self,
        stmt: str,
        bind_params: SqlBindParameters,
        isolation_level: Optional[SqlIsolationLevel] = None,
        system_tags: SqlRequestTagSet = SqlRequestTagSet(),
        extra_tags: SqlJsonTag = SqlJsonTag(),
    ) -> pd.DataFrame:
        with self._concurrency_lock:
            return super()._engine_specific_query_implementation(
                stmt=stmt, bind_params=bind_params, isolation_level=isolation_level
            )

    def _engine_specific_execute_implementation(
        self,
        stmt: str,
        bind_params: SqlBindParameters,
        isolation_level: Optional[SqlIsolationLevel] = None,
        system_tags: SqlRequestTagSet = SqlRequestTagSet(),
        extra_tags: SqlJsonTag = SqlJsonTag(),
    ) -> None:
        with self._concurrency_lock:
            return super()._engine_specific_execute_implementation(
                stmt=stmt, bind_params=bind_params, isolation_level=isolation_level
            )

    def _engine_specific_dry_run_implementation(self, stmt: str, bind_params: SqlBindParameters) -> None:  # noqa: D
        with self._concurrency_lock:
            return super()._engine_specific_dry_run_implementation(stmt=stmt, bind_params=bind_params)

    def create_table_from_dataframe(  # noqa: D
        self, sql_table: SqlTable, df: pd.DataFrame, chunk_size: Optional[int] = None
    ) -> None:
        with self._concurrency_lock:
            raw_connection = self._engine.raw_connection()
            try:
                logger.info(
                    f"Creating table '{sql_table.sql}' from a DataFrame with {df.shape[0]} row(s)"
                )
                start_time = time.time()

                # For DuckDB, create table in the correct schema without catalog prefix
                # First ensure schema exists
                try:
                    raw_connection.execute(f"CREATE SCHEMA IF NOT EXISTS {sql_table.schema_name}")
                    raw_connection.commit()
                except Exception as e:
                    logger.warning(f"Failed to create schema {sql_table.schema_name}: {e}")

                # Then create table directly using SQL to avoid catalog prefix issues
                try:
                    # Create table structure (suppress warning by using warnings module)
                    temp_table = f"temp_{sql_table.table_name}"
                    import warnings
                    with warnings.catch_warnings():
                        warnings.simplefilter("ignore", UserWarning)
                        df.to_sql(name=temp_table, con=raw_connection, schema=None, index=False, if_exists="replace")

                    # Move table to correct schema
                    raw_connection.execute(f"CREATE TABLE {sql_table.sql} AS SELECT * FROM {temp_table}")
                    raw_connection.execute(f"DROP TABLE {temp_table}")
                    raw_connection.commit()
                    logger.info(f"Successfully created table in schema {sql_table.schema_name}")
                except Exception as e:
                    logger.warning(f"DuckDB schema handling failed, using default: {e}")
                    # Fallback to original method
                    df.to_sql(
                        name=sql_table.table_name,
                        con=raw_connection,
                        schema=sql_table.schema_name,
                        index=False,
                        if_exists="fail",
                        method="multi",
                        chunksize=chunk_size,
                    )
                raw_connection.commit()
                logger.info(
                    f"Created table '{sql_table.sql}' from a DataFrame in {time.time() - start_time:.2f}s"
                )
            finally:
                raw_connection.close()

    def cancel_request(self, match_function: Callable[[CombinedSqlTags], bool]) -> int:  # noqa: D
        raise NotImplementedError

    def list_tables(self, schema_name: str) -> Sequence[str]:  # noqa: D
        with self._concurrency_lock:
            insp = inspect(self._engine)
            return insp.get_table_names(schema=schema_name)
