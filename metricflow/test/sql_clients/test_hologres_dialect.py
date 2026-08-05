"""Tests that a Datus `hologres` datasource executes through the PostgreSQL client.

Hologres speaks the PostgreSQL wire protocol and has no MetricFlow dialect of its
own, so `type: hologres` is mapped to the `postgresql` dialect at config-build
time. No database connection is made here — the client factory is patched, and the
unpatched construction test relies on SQLAlchemy engines being lazy.
"""

from unittest import mock

import pytest

from metricflow.configuration.constants import CONFIG_DWH_DIALECT
from metricflow.configuration.dict_config_handler import (
    DictConfigHandler,
    build_config_dict_from_datus_datasource,
)
from metricflow.sql_clients.common_client import SqlDialect
from metricflow.sql_clients.postgres import PostgresSqlClient
from metricflow.sql_clients.sql_utils import make_sql_client_from_config

HOLOGRES_DATASOURCE = {
    "type": "hologres",
    "host": "holo-host",
    "port": 80,
    "username": "holo_user",
    "password": "holo_pw",
    "database": "njyh",
    "sslmode": "require",
}


def _handler(**overrides) -> DictConfigHandler:
    return DictConfigHandler(build_config_dict_from_datus_datasource({**HOLOGRES_DATASOURCE, **overrides}))


class TestHologresClientSelection:
    def test_config_uses_postgresql_dialect(self) -> None:
        assert _handler().get_value(CONFIG_DWH_DIALECT) == SqlDialect.POSTGRESQL.value

    def test_factory_builds_postgres_client_with_sslmode(self) -> None:
        with mock.patch.object(PostgresSqlClient, "from_connection_details") as from_connection_details:
            make_sql_client_from_config(_handler())

        from_connection_details.assert_called_once_with(
            "postgresql://holo_user@holo-host:80/njyh?sslmode=require", "holo_pw"
        )

    def test_factory_omits_sslmode_when_not_configured(self) -> None:
        with mock.patch.object(PostgresSqlClient, "from_connection_details") as from_connection_details:
            make_sql_client_from_config(_handler(sslmode=""))

        from_connection_details.assert_called_once_with("postgresql://holo_user@holo-host:80/njyh", "holo_pw")

    def test_factory_returns_postgres_client_instance(self) -> None:
        pytest.importorskip("psycopg2")

        client = make_sql_client_from_config(_handler())

        assert isinstance(client, PostgresSqlClient)
