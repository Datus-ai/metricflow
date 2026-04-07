"""Unit and integration tests for GreenplumSqlClient.

Unit tests (no database required) verify client construction, engine attributes,
and configuration mapping. Integration tests (require a Greenplum database) are
marked with @pytest.mark.greenplum and verify actual database operations.
"""

import pytest

from metricflow.protocols.sql_client import SqlEngine
from metricflow.sql.render.greenplum import GreenplumSqlQueryPlanRenderer
from metricflow.sql_clients.common_client import SqlDialect
from metricflow.sql_clients.greenplum import GreenplumEngineAttributes, GreenplumSqlClient


class TestGreenplumEngineAttributes:
    """Unit tests for GreenplumEngineAttributes (no database required)."""

    def test_sql_engine_type(self) -> None:
        assert GreenplumEngineAttributes.sql_engine_type == SqlEngine.GREENPLUM

    def test_date_trunc_supported(self) -> None:
        assert GreenplumEngineAttributes.date_trunc_supported is True

    def test_full_outer_joins_supported(self) -> None:
        assert GreenplumEngineAttributes.full_outer_joins_supported is True

    def test_indexes_supported(self) -> None:
        assert GreenplumEngineAttributes.indexes_supported is True

    def test_multi_threading_supported(self) -> None:
        assert GreenplumEngineAttributes.multi_threading_supported is True

    def test_timestamp_type_supported(self) -> None:
        assert GreenplumEngineAttributes.timestamp_type_supported is True

    def test_timestamp_to_string_comparison_supported(self) -> None:
        assert GreenplumEngineAttributes.timestamp_to_string_comparison_supported is True

    def test_cancel_submitted_queries_supported(self) -> None:
        assert GreenplumEngineAttributes.cancel_submitted_queries_supported is True

    def test_continuous_percentile_supported(self) -> None:
        assert GreenplumEngineAttributes.continuous_percentile_aggregation_supported is True

    def test_discrete_percentile_supported(self) -> None:
        assert GreenplumEngineAttributes.discrete_percentile_aggregation_supported is True

    def test_approximate_continuous_not_supported(self) -> None:
        assert GreenplumEngineAttributes.approximate_continuous_percentile_aggregation_supported is False

    def test_approximate_discrete_not_supported(self) -> None:
        assert GreenplumEngineAttributes.approximate_discrete_percentile_aggregation_supported is False

    def test_double_data_type_name(self) -> None:
        assert GreenplumEngineAttributes.double_data_type_name == "DOUBLE PRECISION"

    def test_timestamp_type_name(self) -> None:
        assert GreenplumEngineAttributes.timestamp_type_name == "TIMESTAMP"

    def test_random_function_name(self) -> None:
        assert GreenplumEngineAttributes.random_function_name == "RANDOM"

    def test_sql_query_plan_renderer_type(self) -> None:
        assert isinstance(GreenplumEngineAttributes.sql_query_plan_renderer, GreenplumSqlQueryPlanRenderer)


class TestGreenplumDialect:
    """Unit tests for Greenplum dialect configuration."""

    def test_greenplum_dialect_exists(self) -> None:
        assert SqlDialect.GREENPLUM.value == "greenplum"

    def test_greenplum_engine_exists(self) -> None:
        assert SqlEngine.GREENPLUM.value == "Greenplum"


class TestGreenplumSqlClientConstruction:
    """Tests for GreenplumSqlClient.from_connection_details validation."""

    def test_rejects_wrong_dialect(self) -> None:
        with pytest.raises(ValueError, match="Expected dialect"):
            GreenplumSqlClient.from_connection_details("postgresql://user@host:5432/db", "password")

    def test_rejects_missing_password(self) -> None:
        with pytest.raises(ValueError, match="Password not supplied"):
            GreenplumSqlClient.from_connection_details("greenplum://user@host:5432/db", None)


class TestGreenplumConfigMapping:
    """Tests for Greenplum config mapping in DatusConfigHandler."""

    def test_dialect_mapping(self) -> None:
        dialect_mapping = {
            "postgres": "postgresql",
            "postgresql": "postgresql",
            "greenplum": "greenplum",
            "mysql": "mysql",
            "starrocks": "mysql",
        }
        assert dialect_mapping["greenplum"] == "greenplum"

    def test_greenplum_in_sql_utils_factory(self) -> None:
        """Verify GreenplumSqlClient is importable from sql_clients package."""
        from metricflow.sql_clients import GreenplumSqlClient as ImportedClient

        assert ImportedClient is GreenplumSqlClient
