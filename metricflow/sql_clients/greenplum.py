import logging
import textwrap
from typing import ClassVar, Mapping, Optional, Sequence, Union, Callable

import sqlalchemy

from metricflow.protocols.sql_client import SqlEngine, SqlIsolationLevel
from metricflow.protocols.sql_client import SqlEngineAttributes
from metricflow.protocols.sql_request import SqlRequestTagSet
from metricflow.sql.render.greenplum import GreenplumSqlQueryPlanRenderer
from metricflow.sql.render.sql_plan_renderer import SqlQueryPlanRenderer
from metricflow.sql_clients.async_request import SqlStatementCommentMetadata, CombinedSqlTags
from metricflow.sql_clients.common_client import SqlDialect, not_empty
from metricflow.sql_clients.sqlalchemy_dialect import SqlAlchemySqlClient

logger = logging.getLogger(__name__)


class GreenplumEngineAttributes:
    """Engine-specific attributes for the Greenplum query engine."""

    sql_engine_type: ClassVar[SqlEngine] = SqlEngine.GREENPLUM

    # SQL Engine capabilities
    supported_isolation_levels: ClassVar[Sequence[SqlIsolationLevel]] = ()
    date_trunc_supported: ClassVar[bool] = True
    full_outer_joins_supported: ClassVar[bool] = True
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
    double_data_type_name: ClassVar[str] = "DOUBLE PRECISION"
    timestamp_type_name: ClassVar[Optional[str]] = "TIMESTAMP"
    random_function_name: ClassVar[str] = "RANDOM"

    # MetricFlow attributes
    sql_query_plan_renderer: ClassVar[SqlQueryPlanRenderer] = GreenplumSqlQueryPlanRenderer()


class GreenplumSqlClient(SqlAlchemySqlClient):
    """Implements Greenplum.

    Greenplum uses the PostgreSQL wire protocol, so we connect via postgresql+psycopg2.
    """

    @staticmethod
    def from_connection_details(url: str, password: Optional[str]) -> SqlAlchemySqlClient:  # noqa: D
        parsed_url = sqlalchemy.engine.url.make_url(url)
        dialect = SqlDialect.GREENPLUM.value
        if parsed_url.drivername != dialect:
            raise ValueError(f"Expected dialect '{dialect}' in {url}")

        if password is None:
            raise ValueError(f"Password not supplied for {url}")

        return GreenplumSqlClient(
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
        # Greenplum uses PostgreSQL protocol for connections
        super().__init__(
            engine=self.create_engine(
                dialect="postgresql",
                driver="psycopg2",
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
        """Collection of attributes and features specific to the Greenplum SQL engine."""
        return GreenplumEngineAttributes()

    def list_tables(self, schema_name: str) -> Sequence[str]:  # noqa: D
        # Greenplum 4.x (PG 8.2) lacks pg_class.relpersistence, so SQLAlchemy's
        # inspect().get_table_names() fails. Use a compatible query instead.
        df = self.query(
            f"SELECT tablename FROM pg_tables WHERE schemaname = '{schema_name}' ORDER BY tablename"
        )
        return list(df["tablename"])

    def create_schema(self, schema_name: str) -> None:  # noqa: D
        # Greenplum 4.x (PG 8.2) does not support CREATE SCHEMA IF NOT EXISTS.
        # Check existence first, then create if needed.
        result = self.query(
            f"SELECT 1 FROM pg_namespace WHERE nspname = '{schema_name}'"
        )
        if len(result) == 0:
            self.execute(f"CREATE SCHEMA {schema_name}")

    def cancel_submitted_queries(self) -> None:  # noqa: D
        for request_id in self.active_requests():
            self.cancel_request(SqlRequestTagSet.create_from_request_id(request_id))

    def cancel_request(self, match_function: Callable[[CombinedSqlTags], bool]) -> int:  # noqa: D
        result = self.query(
            textwrap.dedent(
                """\
                SELECT pid AS query_id, query AS query_text
                FROM pg_stat_activity
                WHERE query != '<IDLE>' AND query NOT ILIKE '%pg_stat_activity%'
                ORDER BY query_start desc;
                """
            )
        )

        num_cancelled_queries = 0

        for query_id, query_text in result.values:
            parsed_tags = SqlStatementCommentMetadata.parse_tag_metadata_in_comments(query_text)

            if match_function(parsed_tags):
                logger.info(f"Cancelling query ID: {query_id}")
                self.execute(f"SELECT pg_cancel_backend({query_id});")
                num_cancelled_queries += 1

        return num_cancelled_queries
