"""Schema-validator tests for the locked_metadata.uid field.

uid is the stable node identity assigned by the Semantic Hub; it is written into
both metric and data_source YAML, so the validators must accept it (and the
data_source spec must accept a locked_metadata block at all) while still
rejecting genuinely unknown keys.
"""

import pytest
from jsonschema.exceptions import ValidationError

from metricflow.model.parsing.schemas_internal import (
    data_source_validator,
    metric_validator,
)


def test_metric_locked_metadata_accepts_uid() -> None:
    metric_validator.validate(
        {
            "name": "bookings",
            "type": "measure_proxy",
            "type_params": {"measures": ["bookings"]},
            "locked_metadata": {"uid": "01HZX9Q2K7", "owner": "arno@datus.ai", "tags": ["t"]},
        }
    )


def test_data_source_accepts_locked_metadata_uid() -> None:
    data_source_validator.validate(
        {
            "name": "id_verifications",
            "sql_table": "fct_id_verifications",
            "locked_metadata": {"uid": "01HZX9Q2K7", "owner": "arno@datus.ai"},
        }
    )


def test_locked_metadata_still_rejects_unknown_key() -> None:
    with pytest.raises(ValidationError):
        metric_validator.validate(
            {
                "name": "bookings",
                "type": "measure_proxy",
                "type_params": {"measures": ["bookings"]},
                "locked_metadata": {"bogus_field": "x"},
            }
        )
