from contextlib import contextmanager
import json
import threading
from typing import Any

import pytest
import sqlalchemy

from metricflow.configuration.constants import (
    CONFIG_DWH_ACCOUNT,
    CONFIG_DWH_DB,
    CONFIG_DWH_DIALECT,
    CONFIG_DWH_HOST,
    CONFIG_DWH_PASSWORD,
    CONFIG_DWH_PRIVATE_KEY,
    CONFIG_DWH_PRIVATE_KEY_FILE,
    CONFIG_DWH_PRIVATE_KEY_FILE_PWD,
    CONFIG_DWH_ROLE,
    CONFIG_DWH_USER,
    CONFIG_DWH_WAREHOUSE,
)
from metricflow.configuration.dict_config_handler import DictConfigHandler
from metricflow.protocols.sql_client import SqlEngine
from metricflow.protocols.sql_request import MF_EXTRA_TAGS_KEY, SqlJsonTag
from metricflow.sql_clients.snowflake import SnowflakeSqlClient
from metricflow.sql_clients.sql_utils import make_sql_client, make_sql_client_from_config
from metricflow.sql_clients.sqlalchemy_dialect import SqlAlchemySqlClient


def _private_key_pem(passphrase: bytes | None = None) -> tuple[str, bytes]:
    serialization = pytest.importorskip("cryptography.hazmat.primitives.serialization")
    rsa = pytest.importorskip("cryptography.hazmat.primitives.asymmetric.rsa")

    private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    if passphrase:
        encryption_algorithm = serialization.BestAvailableEncryption(passphrase)
    else:
        encryption_algorithm = serialization.NoEncryption()

    pem = private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=encryption_algorithm,
    ).decode("utf-8")
    der = private_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )
    return pem, der


def _stub_snowflake_engine(monkeypatch: pytest.MonkeyPatch) -> None:
    def _create_engine(
        self: SnowflakeSqlClient,
        login_timeout: int = SnowflakeSqlClient.DEFAULT_LOGIN_TIMEOUT,
        client_session_keep_alive: bool = SnowflakeSqlClient.DEFAULT_CLIENT_SESSION_KEEP_ALIVE,
    ) -> sqlalchemy.engine.Engine:
        return sqlalchemy.create_engine("sqlite://")

    monkeypatch.setattr(SnowflakeSqlClient, "_create_engine", _create_engine)


def test_make_sql_client_supports_snowflake_url(monkeypatch: pytest.MonkeyPatch) -> None:
    _stub_snowflake_engine(monkeypatch)

    sql_client = make_sql_client("snowflake://sf_user@my_account/sf_db?warehouse=wh1", "sf_pw")

    assert isinstance(sql_client, SnowflakeSqlClient)
    assert sql_client.sql_engine_attributes.sql_engine_type is SqlEngine.SNOWFLAKE
    assert sql_client._connection_url.drivername == "snowflake"
    assert sql_client._connection_url.username == "sf_user"
    assert sql_client._connection_url.host == "my_account"
    assert sql_client._connection_url.database == "sf_db"
    assert sql_client._connection_url.query["warehouse"] == "wh1"
    sql_client.close()


def test_make_sql_client_supports_snowflake_key_pair_url(monkeypatch: pytest.MonkeyPatch) -> None:
    _stub_snowflake_engine(monkeypatch)

    sql_client = make_sql_client(
        "snowflake://sf_user@my_account/sf_db?warehouse=wh1"
        "&authenticator=SNOWFLAKE_JWT&private_key_file=%2Ftmp%2Frsa_key.p8",
        "",
    )

    assert isinstance(sql_client, SnowflakeSqlClient)
    assert sql_client._connection_url.password is None
    assert "authenticator" not in sql_client._connection_url.query
    assert "private_key_file" not in sql_client._connection_url.query
    assert sql_client._auth_connect_args == {
        "authenticator": "SNOWFLAKE_JWT",
        "private_key_file": "/tmp/rsa_key.p8",
    }
    sql_client.close()


def test_snowflake_key_pair_passphrase_uses_explicit_argument(monkeypatch: pytest.MonkeyPatch) -> None:
    _stub_snowflake_engine(monkeypatch)

    sql_client = SnowflakeSqlClient.from_connection_details(
        "snowflake://sf_user@my_account/sf_db?warehouse=wh1"
        "&authenticator=SNOWFLAKE_JWT&private_key_file=%2Ftmp%2Frsa_key.p8",
        "",
        private_key_file_pwd="key-pass",
    )

    assert sql_client._auth_connect_args == {
        "authenticator": "SNOWFLAKE_JWT",
        "private_key_file": "/tmp/rsa_key.p8",
        "private_key_file_pwd": "key-pass",
    }
    sql_client.close()


def test_snowflake_private_key_argument_uses_der_connect_arg(monkeypatch: pytest.MonkeyPatch) -> None:
    _stub_snowflake_engine(monkeypatch)
    private_key, expected_der = _private_key_pem()

    sql_client = SnowflakeSqlClient.from_connection_details(
        "snowflake://sf_user@my_account/sf_db?warehouse=wh1",
        None,
        private_key=private_key.replace("\n", "\\n"),
    )

    assert sql_client._auth_connect_args == {
        "authenticator": "SNOWFLAKE_JWT",
        "private_key": expected_der,
    }
    sql_client.close()


def test_snowflake_private_key_passphrase_decrypts_pem(monkeypatch: pytest.MonkeyPatch) -> None:
    _stub_snowflake_engine(monkeypatch)
    private_key, expected_der = _private_key_pem(passphrase=b"key-pass")

    sql_client = SnowflakeSqlClient.from_connection_details(
        "snowflake://sf_user@my_account/sf_db?warehouse=wh1",
        None,
        private_key=private_key,
        private_key_file_pwd="key-pass",
    )

    assert sql_client._auth_connect_args == {
        "authenticator": "SNOWFLAKE_JWT",
        "private_key": expected_der,
    }
    sql_client.close()


def test_make_sql_client_rejects_missing_snowflake_credentials(monkeypatch: pytest.MonkeyPatch) -> None:
    _stub_snowflake_engine(monkeypatch)

    with pytest.raises(ValueError, match="exactly one of password, private_key, or private_key_file"):
        make_sql_client("snowflake://sf_user@my_account/sf_db?warehouse=wh1", "")


def test_make_sql_client_rejects_key_passphrase_in_url(monkeypatch: pytest.MonkeyPatch) -> None:
    _stub_snowflake_engine(monkeypatch)

    with pytest.raises(ValueError, match="private_key_file_pwd must be supplied via the explicit argument"):
        make_sql_client("snowflake://sf_user@my_account/sf_db?warehouse=wh1&private_key_file_pwd=key-pass", "")


def test_make_sql_client_rejects_multiple_snowflake_credentials(monkeypatch: pytest.MonkeyPatch) -> None:
    _stub_snowflake_engine(monkeypatch)

    with pytest.raises(ValueError, match="exactly one of password, private_key, or private_key_file"):
        make_sql_client(
            "snowflake://sf_user@my_account/sf_db?warehouse=wh1&private_key_file=%2Ftmp%2Frsa_key.p8",
            "sf_pw",
        )


def test_snowflake_key_passphrase_requires_key_material(monkeypatch: pytest.MonkeyPatch) -> None:
    _stub_snowflake_engine(monkeypatch)

    with pytest.raises(ValueError, match="private_key_file_pwd requires private_key or private_key_file"):
        SnowflakeSqlClient.from_connection_details(
            "snowflake://sf_user@my_account/sf_db?warehouse=wh1",
            None,
            private_key_file_pwd="key-pass",
        )


def test_make_sql_client_from_config_prefers_snowflake_account(monkeypatch: pytest.MonkeyPatch) -> None:
    _stub_snowflake_engine(monkeypatch)
    handler = DictConfigHandler(
        {
            CONFIG_DWH_DIALECT: "snowflake",
            CONFIG_DWH_ACCOUNT: "configured_account",
            CONFIG_DWH_HOST: "legacy_host",
            CONFIG_DWH_USER: "sf_user",
            CONFIG_DWH_PASSWORD: "sf_pw",
            CONFIG_DWH_DB: "sf_db",
            CONFIG_DWH_WAREHOUSE: "wh1",
            CONFIG_DWH_ROLE: "analytics_role",
        }
    )

    sql_client = make_sql_client_from_config(handler)

    assert isinstance(sql_client, SnowflakeSqlClient)
    assert sql_client._connection_url.host == "configured_account"
    assert sql_client._connection_url.query["warehouse"] == "wh1"
    assert sql_client._connection_url.query["role"] == "analytics_role"
    sql_client.close()


def test_make_sql_client_from_config_supports_snowflake_key_pair(monkeypatch: pytest.MonkeyPatch) -> None:
    _stub_snowflake_engine(monkeypatch)
    handler = DictConfigHandler(
        {
            CONFIG_DWH_DIALECT: "snowflake",
            CONFIG_DWH_ACCOUNT: "configured_account",
            CONFIG_DWH_USER: "sf_user",
            CONFIG_DWH_DB: "sf_db",
            CONFIG_DWH_WAREHOUSE: "wh1",
            CONFIG_DWH_PRIVATE_KEY_FILE: "/tmp/rsa_key.p8",
            CONFIG_DWH_PRIVATE_KEY_FILE_PWD: "key-pass",
        }
    )

    sql_client = make_sql_client_from_config(handler)

    assert isinstance(sql_client, SnowflakeSqlClient)
    assert sql_client._connection_url.password is None
    assert sql_client._connection_url.host == "configured_account"
    assert sql_client._auth_connect_args == {
        "authenticator": "SNOWFLAKE_JWT",
        "private_key_file": "/tmp/rsa_key.p8",
        "private_key_file_pwd": "key-pass",
    }
    sql_client.close()


def test_make_sql_client_from_config_supports_snowflake_private_key(monkeypatch: pytest.MonkeyPatch) -> None:
    _stub_snowflake_engine(monkeypatch)
    private_key, expected_der = _private_key_pem(passphrase=b"key-pass")
    handler = DictConfigHandler(
        {
            CONFIG_DWH_DIALECT: "snowflake",
            CONFIG_DWH_ACCOUNT: "configured_account",
            CONFIG_DWH_USER: "sf_user",
            CONFIG_DWH_DB: "sf_db",
            CONFIG_DWH_WAREHOUSE: "wh1",
            CONFIG_DWH_PRIVATE_KEY: private_key,
            CONFIG_DWH_PRIVATE_KEY_FILE_PWD: "key-pass",
        }
    )

    sql_client = make_sql_client_from_config(handler)

    assert isinstance(sql_client, SnowflakeSqlClient)
    assert sql_client._connection_url.password is None
    assert sql_client._connection_url.host == "configured_account"
    assert sql_client._auth_connect_args == {
        "authenticator": "SNOWFLAKE_JWT",
        "private_key": expected_der,
    }
    sql_client.close()


def test_make_sql_client_from_config_uses_host_when_account_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    _stub_snowflake_engine(monkeypatch)
    handler = DictConfigHandler(
        {
            CONFIG_DWH_DIALECT: "snowflake",
            CONFIG_DWH_HOST: "host_account",
            CONFIG_DWH_USER: "sf_user",
            CONFIG_DWH_PASSWORD: "sf_pw",
            CONFIG_DWH_DB: "sf_db",
            CONFIG_DWH_WAREHOUSE: "wh1",
        }
    )

    sql_client = make_sql_client_from_config(handler)

    assert isinstance(sql_client, SnowflakeSqlClient)
    assert sql_client._connection_url.host == "host_account"
    sql_client.close()


def test_make_sql_client_from_config_requires_snowflake_warehouse() -> None:
    handler = DictConfigHandler(
        {
            CONFIG_DWH_DIALECT: "snowflake",
            CONFIG_DWH_ACCOUNT: "configured_account",
            CONFIG_DWH_USER: "sf_user",
            CONFIG_DWH_PASSWORD: "sf_pw",
            CONFIG_DWH_DB: "sf_db",
        }
    )

    with pytest.raises(ValueError, match="Missing warehouse"):
        make_sql_client_from_config(handler)


def test_snowflake_engine_connection_uses_sqlalchemy_text_and_cleans_up_session(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class FakeConnection:
        def __init__(self) -> None:
            self.calls: list[tuple[Any, dict[str, str]]] = []

        def execute(self, statement: Any, parameters: dict[str, str] | None = None):
            self.calls.append((statement, parameters or {}))
            assert not isinstance(statement, str)
            if "CURRENT_SESSION" in str(statement):
                return [(12345,)]
            return []

    fake_conn = FakeConnection()

    @contextmanager
    def _fake_base_connection(self, engine, isolation_level=None, system_tags=None, extra_tags=None):
        yield fake_conn

    monkeypatch.setattr(SqlAlchemySqlClient, "_engine_connection", _fake_base_connection)
    sql_client = SnowflakeSqlClient.__new__(SnowflakeSqlClient)
    sql_client._engine = object()
    sql_client._known_sessions_ids_lock = threading.Lock()
    sql_client._known_session_ids = set()

    with pytest.raises(RuntimeError, match="boom"):
        with sql_client._engine_connection(sql_client._engine, extra_tags=SqlJsonTag({"request": "abc"})):
            assert sql_client._known_session_ids == {12345}
            raise RuntimeError("boom")

    assert sql_client._known_session_ids == set()
    assert str(fake_conn.calls[0][0]) == "ALTER SESSION SET WEEK_START = 1;"
    assert str(fake_conn.calls[1][0]) == "ALTER SESSION SET QUERY_TAG = :query_tag"
    assert json.loads(fake_conn.calls[1][1]["query_tag"])[MF_EXTRA_TAGS_KEY] == {"request": "abc"}
    assert str(fake_conn.calls[2][0]) == "SELECT CURRENT_SESSION()"
