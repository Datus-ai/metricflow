from __future__ import annotations

from typing import Optional, List
from pydantic import field_validator
from metricflow.aggregation_properties import AggregationType
from metricflow.model.objects.common import Metadata
from metricflow.model.objects.base import ModelWithMetadataParsing, HashableBaseModel
from metricflow.references import MeasureReference, TimeDimensionReference


class NonAdditiveDimensionParameters(HashableBaseModel):
    """Describes the params for specifying non-additive dimensions in a measure.

    NOTE: Currently, only TimeDimensions are supported for this filter
    """

    name: str

    # Optional Fields
    window_choice: AggregationType = AggregationType.MIN
    window_groupings: List[str] = []


class MeasureAggregationParameters(HashableBaseModel):
    """Describes parameters for aggregations"""

    percentile: Optional[float] = None
    use_discrete_percentile: bool = False
    use_approximate_percentile: bool = False


class Measure(HashableBaseModel, ModelWithMetadataParsing):
    """Describes a measure"""

    name: str
    agg: AggregationType
    description: Optional[str] = None
    create_metric: Optional[bool] = None
    expr: Optional[str] = None
    agg_params: Optional[MeasureAggregationParameters] = None
    metadata: Optional[Metadata] = None
    non_additive_dimension: Optional[NonAdditiveDimensionParameters] = None

    @field_validator("expr", mode="before")
    @classmethod
    def coerce_expr_to_str(cls, v):  # noqa: D102
        if v is not None and not isinstance(v, str):
            return str(v)
        return v

    # Defines the time dimension to aggregate this measure by. If not specified, it means to use the primary time
    # dimension in the data source.
    agg_time_dimension: Optional[str] = None

    @property
    def checked_agg_time_dimension(self) -> TimeDimensionReference:
        """Returns the aggregation time dimension, throwing an exception if it's not set."""
        assert self.agg_time_dimension, (
            f"Aggregation time dimension for measure {self.name} is not set! This should either be set directly on "
            f"the measure specification in the model, or else defaulted to the primary time dimension in the data "
            f"source containing the measure."
        )
        return TimeDimensionReference(element_name=self.agg_time_dimension)

    @property
    def reference(self) -> MeasureReference:  # noqa: D
        return MeasureReference(element_name=self.name)
