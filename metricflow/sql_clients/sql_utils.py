from typing import Any, List, Optional, Set

import dateutil.parser
import pandas as pd
from sqlalchemy.engine import make_url

from metricflow.configuration.constants import (
    CONFIG_DWH_DB,
    CONFIG_DWH_DIALECT,
    CONFIG_DWH_HOST,
    CONFIG_DWH_PORT,
    CONFIG_DWH_USER,
    CONFIG_DWH_PASSWORD,
)
from metricflow.configuration.yaml_handler import YamlFileHandler
from metricflow.protocols.async_sql_client import AsyncSqlClient
from metricflow.protocols.sql_client import SqlClient, SqlIsolationLevel
from metricflow.protocols.sql_request import SqlJsonTag
from metricflow.sql.sql_bind_parameters import SqlBindParameters
from metricflow.sql_clients.base_sql_client_implementation import SqlClientException
from metricflow.sql_clients.common_client import SqlDialect, not_empty
from metricflow.sql_clients.duckdb import DuckDbSqlClient
from metricflow.sql_clients.mysql import MySQLSqlClient
from metricflow.sql_clients.sqlite import SqliteSqlClient


def make_df(  # type: ignore [misc]
    sql_client: SqlClient, columns: List[str], data: Any, time_columns: Optional[Set[str]] = None
) -> pd.DataFrame:
    """Helper to make a dataframe, converting the time columns to appropriate types."""
    time_columns = time_columns or set()
    # Should only be used in testing, so sql_client should be set.
    assert sql_client

    if sql_client.sql_engine_attributes.timestamp_type_supported:
        new_rows = []
        for row in data:
            new_row = []
            # Change the type of the column if it's in time_columns
            for i, column in enumerate(columns):
                if column in time_columns and row[i] is not None:
                    # ts_suffix = " 00:00:00" if ":" not in row[i] else ""
                    # ts_input = row[i] + ts_suffix
                    new_row.append(dateutil.parser.parse(row[i]))

                else:
                    new_row.append(row[i])
            new_rows.append(new_row)
        data = new_rows

    return pd.DataFrame(
        columns=columns,
        data=data,
    )


def make_sql_client(url: str, password: str) -> AsyncSqlClient:
    """Build SQL client based on env configs. Used only in tests."""
    dialect_protocol = make_url(url.split(";")[0]).drivername.split("+")
    dialect = SqlDialect(dialect_protocol[0])
    if len(dialect_protocol) > 2:
        raise ValueError(f"Invalid # of +'s in {url}")

    if dialect == SqlDialect.DUCKDB:
        return DuckDbSqlClient.from_connection_details(url, password)
    elif dialect == SqlDialect.MYSQL:
        return MySQLSqlClient.from_connection_details(url, password)
    elif dialect == SqlDialect.SQLITE:
        return SqliteSqlClient.from_connection_details(url, password)
    else:
        raise ValueError(
            f"Only DuckDB, MySQL, and SQLite dialects are supported in this build. Got: `{dialect}` in URL {url}"
        )


def make_sql_client_from_config(handler: YamlFileHandler) -> AsyncSqlClient:
    """Construct a SqlClient given a yaml file config."""

    url = handler.url
    dialect = not_empty(handler.get_value(CONFIG_DWH_DIALECT), CONFIG_DWH_DIALECT, url).lower()
    if dialect == SqlDialect.DUCKDB.value:
        database = not_empty(handler.get_value(CONFIG_DWH_DB), CONFIG_DWH_DB, url)
        return DuckDbSqlClient(file_path=database)
    elif dialect == SqlDialect.MYSQL.value:
        # For MySQL, we need to construct a connection URL from config components
        host = not_empty(handler.get_value(CONFIG_DWH_HOST), "host", url)
        port = not_empty(handler.get_value(CONFIG_DWH_PORT), "port", url)
        username = not_empty(handler.get_value(CONFIG_DWH_USER), "username", url)
        password = not_empty(handler.get_value(CONFIG_DWH_PASSWORD), "password", url)
        database = not_empty(handler.get_value(CONFIG_DWH_DB), "database", url)

        # Construct MySQL URL
        mysql_url = f"mysql://{username}@{host}:{port}/{database}"
        return MySQLSqlClient.from_connection_details(mysql_url, password)
    elif dialect == SqlDialect.SQLITE.value:
        database = not_empty(handler.get_value(CONFIG_DWH_DB), CONFIG_DWH_DB, url)
        return SqliteSqlClient(file_path=database)
    else:
        raise ValueError(
            f"Only DuckDB, MySQL, and SQLite dialects are supported in this build. Got dialect '{dialect}' in {url}"
        )


def sync_execute(  # noqa: D
    async_sql_client: AsyncSqlClient,
    statement: str,
    bind_parameters: SqlBindParameters = SqlBindParameters(),
    extra_sql_tags: SqlJsonTag = SqlJsonTag(),
    isolation_level: Optional[SqlIsolationLevel] = None,
) -> None:
    request_id = async_sql_client.async_execute(
        statement=statement,
        bind_parameters=bind_parameters,
        extra_tags=extra_sql_tags,
        isolation_level=isolation_level,
    )

    result = async_sql_client.async_request_result(request_id)
    if result.exception:
        raise SqlClientException(
            f"Got an exception when trying to execute a statement: {result.exception}"
        ) from result.exception
    return


def sync_query(  # noqa: D
    async_sql_client: AsyncSqlClient,
    statement: str,
    bind_parameters: SqlBindParameters = SqlBindParameters(),
    extra_sql_tags: SqlJsonTag = SqlJsonTag(),
    isolation_level: Optional[SqlIsolationLevel] = None,
) -> pd.DataFrame:
    request_id = async_sql_client.async_query(
        statement=statement,
        bind_parameters=bind_parameters,
        extra_tags=extra_sql_tags,
        isolation_level=isolation_level,
    )

    result = async_sql_client.async_request_result(request_id)
    if result.exception:
        raise SqlClientException(
            f"Got an exception when trying to execute a statement: {result.exception}"
        ) from result.exception
    assert result.df is not None, "A dataframe should have been returned if there was no error"
    return result.df
