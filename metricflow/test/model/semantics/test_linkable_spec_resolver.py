import logging
import textwrap
from typing import Sequence

import pytest

from metricflow.model.semantic_model import SemanticModel
from metricflow.model.objects.common import YamlConfigFile
from metricflow.model.parsing.dir_to_model import parse_yaml_files_to_validation_ready_model
from metricflow.model.semantics.linkable_spec_resolver import (
    ValidLinkableSpecResolver,
)
from metricflow.model.semantics.linkable_element_properties import LinkableElementProperties
from metricflow.model.semantics.data_source_join_evaluator import MAX_JOIN_HOPS
from metricflow.references import MetricReference

logger = logging.getLogger(__name__)


@pytest.fixture
def simple_model_spec_resolver(simple_semantic_model: SemanticModel) -> ValidLinkableSpecResolver:  # noqa: D
    return ValidLinkableSpecResolver(
        user_configured_model=simple_semantic_model.user_configured_model,
        data_source_semantics=simple_semantic_model.data_source_semantics,
        max_identifier_links=MAX_JOIN_HOPS,
    )


def test_linkable_spec_resolver(simple_model_spec_resolver: ValidLinkableSpecResolver) -> None:  # noqa: D
    result = simple_model_spec_resolver.get_linkable_elements_for_metrics(
        metric_references=[MetricReference(element_name="bookings"), MetricReference(element_name="views")],
        with_any_of=LinkableElementProperties.all_properties(),
        without_any_of=frozenset({LinkableElementProperties.DERIVED_TIME_GRANULARITY}),
    ).as_spec_set

    assert [
        "create_a_cycle_in_the_join_graph__is_instant",
        "create_a_cycle_in_the_join_graph__listing__capacity_latest",
        "create_a_cycle_in_the_join_graph__listing__country_latest",
        "create_a_cycle_in_the_join_graph__listing__is_lux_latest",
        "listing__capacity_latest",
        "listing__country_latest",
        "listing__is_lux_latest",
        "listing__user__company_name",
        "listing__user__home_state",
        "listing__user__home_state_latest",
    ] == sorted(tuple(x.qualified_name for x in result.dimension_specs))
    assert [
        "create_a_cycle_in_the_join_graph__booking_paid_at",
        "create_a_cycle_in_the_join_graph__listing__created_at",
        "create_a_cycle_in_the_join_graph__listing__ds",
        "ds",
        "ds_partitioned",
        "listing__created_at",
        "listing__ds",
        "listing__user__created_at",
        "listing__user__ds_partitioned",
    ] == sorted(tuple(x.qualified_name for x in result.time_dimension_specs))
    assert [
        "create_a_cycle_in_the_join_graph",
        "create_a_cycle_in_the_join_graph__listing",
        "create_a_cycle_in_the_join_graph__listing__lux_listing",
        "create_a_cycle_in_the_join_graph__listing__user",
        "listing",
        "listing__lux_listing",
        "listing__user",
        "listing__user__company",
    ] == sorted(tuple(x.qualified_name for x in result.identifier_specs))


def property_check_helper(  # noqa: D
    spec_resolver: ValidLinkableSpecResolver,
    metric_references: Sequence[MetricReference],
    element_property: LinkableElementProperties,
    expected_names: Sequence[str],
) -> None:
    results = spec_resolver.get_linkable_elements_for_metrics(
        metric_references=metric_references,
        with_any_of=frozenset({element_property}),
        without_any_of=frozenset(),
    ).as_spec_set.as_tuple

    actual_names = sorted(tuple(x.qualified_name for x in results))
    assert expected_names == actual_names


def test_local_property(simple_model_spec_resolver: ValidLinkableSpecResolver) -> None:  # noqa: D
    property_check_helper(
        spec_resolver=simple_model_spec_resolver,
        metric_references=[MetricReference(element_name="listings")],
        element_property=LinkableElementProperties.LOCAL,
        expected_names=[
            "capacity_latest",
            "country_latest",
            "created_at",
            "created_at__month",
            "created_at__quarter",
            "created_at__week",
            "created_at__year",
            "ds",
            "ds__month",
            "ds__quarter",
            "ds__week",
            "ds__year",
            "is_lux_latest",
            "listing",
            "listing__capacity_latest",
            "listing__country_latest",
            "listing__created_at",
            "listing__created_at__month",
            "listing__created_at__quarter",
            "listing__created_at__week",
            "listing__created_at__year",
            "listing__ds",
            "listing__ds__month",
            "listing__ds__quarter",
            "listing__ds__week",
            "listing__ds__year",
            "listing__is_lux_latest",
            "user",
        ],
    )


def test_local_linked_property(simple_model_spec_resolver: ValidLinkableSpecResolver) -> None:  # noqa: D
    property_check_helper(
        spec_resolver=simple_model_spec_resolver,
        metric_references=[MetricReference(element_name="listings")],
        element_property=LinkableElementProperties.LOCAL_LINKED,
        expected_names=[
            "listing__capacity_latest",
            "listing__country_latest",
            "listing__created_at",
            "listing__created_at__month",
            "listing__created_at__quarter",
            "listing__created_at__week",
            "listing__created_at__year",
            "listing__ds",
            "listing__ds__month",
            "listing__ds__quarter",
            "listing__ds__week",
            "listing__ds__year",
            "listing__is_lux_latest",
        ],
    )


def test_joined_property(simple_model_spec_resolver: ValidLinkableSpecResolver) -> None:  # noqa: D
    property_check_helper(
        spec_resolver=simple_model_spec_resolver,
        metric_references=[MetricReference(element_name="listings")],
        element_property=LinkableElementProperties.JOINED,
        expected_names=[
            "listing__lux_listing",
            "user__company",
            "user__company_name",
            "user__created_at",
            "user__created_at__month",
            "user__created_at__quarter",
            "user__created_at__week",
            "user__created_at__year",
            "user__ds_partitioned",
            "user__ds_partitioned__month",
            "user__ds_partitioned__quarter",
            "user__ds_partitioned__week",
            "user__ds_partitioned__year",
            "user__home_state",
            "user__home_state_latest",
        ],
    )


def test_multi_hop_property(multi_hop_join_semantic_model: SemanticModel) -> None:  # noqa: D
    multi_hop_spec_resolver = ValidLinkableSpecResolver(
        user_configured_model=multi_hop_join_semantic_model.user_configured_model,
        data_source_semantics=multi_hop_join_semantic_model.data_source_semantics,
        max_identifier_links=MAX_JOIN_HOPS,
    )
    property_check_helper(
        spec_resolver=multi_hop_spec_resolver,
        metric_references=[MetricReference(element_name="txn_count")],
        element_property=LinkableElementProperties.MULTI_HOP,
        expected_names=[
            "account_id__customer_id__country",
            "account_id__customer_id__customer_atomic_weight",
            "account_id__customer_id__customer_name",
            "account_id__customer_id__customer_third_hop_id",
            "account_id__customer_id__ds_partitioned",
            "account_id__customer_id__ds_partitioned__month",
            "account_id__customer_id__ds_partitioned__quarter",
            "account_id__customer_id__ds_partitioned__week",
            "account_id__customer_id__ds_partitioned__year",
        ],
    )


def test_three_hop_property(multi_hop_join_semantic_model: SemanticModel) -> None:  # noqa: D
    """Tests that 3-hop linkable elements are discoverable when max_identifier_links is sufficient."""
    three_hop_spec_resolver = ValidLinkableSpecResolver(
        user_configured_model=multi_hop_join_semantic_model.user_configured_model,
        data_source_semantics=multi_hop_join_semantic_model.data_source_semantics,
        max_identifier_links=3,
    )
    result = three_hop_spec_resolver.get_linkable_elements_for_metrics(
        metric_references=[MetricReference(element_name="txn_count")],
        with_any_of=frozenset({LinkableElementProperties.MULTI_HOP}),
        without_any_of=frozenset(),
    ).as_spec_set.as_tuple

    three_hop_names = sorted(x.qualified_name for x in result if len(x.identifier_links) == 3)
    assert "account_id__customer_id__customer_third_hop_id__value" in three_hop_names


def test_five_hop_property(multi_hop_join_semantic_model: SemanticModel) -> None:  # noqa: D
    """Tests that 5-hop linkable elements are discoverable when max_identifier_links is sufficient."""
    five_hop_spec_resolver = ValidLinkableSpecResolver(
        user_configured_model=multi_hop_join_semantic_model.user_configured_model,
        data_source_semantics=multi_hop_join_semantic_model.data_source_semantics,
        max_identifier_links=5,
    )
    result = five_hop_spec_resolver.get_linkable_elements_for_metrics(
        metric_references=[MetricReference(element_name="txn_count")],
        with_any_of=frozenset({LinkableElementProperties.MULTI_HOP}),
        without_any_of=frozenset(),
    ).as_spec_set.as_tuple

    five_hop_names = sorted(x.qualified_name for x in result if len(x.identifier_links) == 5)
    assert (
        "account_id__customer_id__customer_third_hop_id__fourth_hop_id__fifth_hop_id__fifth_hop_value" in five_hop_names
    )


def test_metric_semantics_default_supports_five_hop(multi_hop_join_semantic_model: SemanticModel) -> None:
    """Tests that MetricSemantics' bounded default still supports the intended N-hop use case."""

    result = multi_hop_join_semantic_model.metric_semantics.element_specs_for_metrics(
        metric_references=[MetricReference(element_name="txn_count")],
        with_any_property=frozenset({LinkableElementProperties.MULTI_HOP}),
    )

    assert "account_id__customer_id__customer_third_hop_id__fourth_hop_id__fifth_hop_id__fifth_hop_value" in {
        x.qualified_name for x in result
    }


def test_multi_scd_paths_are_not_linkable() -> None:
    """Tests that a deep path with two validity-window data sources is not advertised as queryable."""

    yaml_contents = textwrap.dedent(
        """\
        data_source:
          name: fact_source
          sql_table: some_schema.fact_source
          measures:
            - name: fact_count
              expr: "1"
              agg: sum
          dimensions:
            - name: ds
              type: time
              type_params:
                is_primary: true
                time_granularity: day
          identifiers:
            - name: acct
              type: foreign
        ---
        data_source:
          name: scd_a
          sql_table: some_schema.scd_a
          dimensions:
            - name: scd_a_value
              type: categorical
            - name: window_start
              type: time
              type_params:
                time_granularity: day
                validity_params:
                  is_start: true
            - name: window_end
              type: time
              type_params:
                time_granularity: day
                validity_params:
                  is_end: true
          identifiers:
            - name: acct
              type: natural
            - name: bridge_key
              type: foreign
        ---
        data_source:
          name: bridge_source
          sql_table: some_schema.bridge_source
          dimensions:
            - name: bridge_value
              type: categorical
          identifiers:
            - name: bridge_key
              type: primary
            - name: scd_key
              type: foreign
        ---
        data_source:
          name: scd_c
          sql_table: some_schema.scd_c
          dimensions:
            - name: scd_c_value
              type: categorical
            - name: window_start
              type: time
              type_params:
                time_granularity: day
                validity_params:
                  is_start: true
            - name: window_end
              type: time
              type_params:
                time_granularity: day
                validity_params:
                  is_end: true
          identifiers:
            - name: scd_key
              type: natural
        ---
        metric:
          name: fact_count
          type: measure_proxy
          type_params:
            measures:
              - fact_count
        """
    )
    semantic_model = SemanticModel(
        parse_yaml_files_to_validation_ready_model(
            [YamlConfigFile(filepath="multi_scd_path_model.yaml", contents=yaml_contents)]
        ).model
    )
    spec_resolver = ValidLinkableSpecResolver(
        user_configured_model=semantic_model.user_configured_model,
        data_source_semantics=semantic_model.data_source_semantics,
        max_identifier_links=3,
    )

    result = spec_resolver.get_linkable_elements_for_metrics(
        metric_references=[MetricReference(element_name="fact_count")],
        with_any_of=LinkableElementProperties.all_properties(),
        without_any_of=frozenset(),
    ).as_spec_set
    dimension_names = {x.qualified_name for x in result.dimension_specs}

    assert "acct__bridge_key__bridge_value" in dimension_names
    assert "acct__bridge_key__scd_key__scd_c_value" not in dimension_names


def test_derived_time_granularity_property(simple_model_spec_resolver: ValidLinkableSpecResolver) -> None:  # noqa: D
    property_check_helper(
        spec_resolver=simple_model_spec_resolver,
        metric_references=[MetricReference(element_name="listings")],
        element_property=LinkableElementProperties.DERIVED_TIME_GRANULARITY,
        expected_names=[
            "created_at__month",
            "created_at__quarter",
            "created_at__week",
            "created_at__year",
            "ds__month",
            "ds__quarter",
            "ds__week",
            "ds__year",
            "listing__created_at__month",
            "listing__created_at__quarter",
            "listing__created_at__week",
            "listing__created_at__year",
            "listing__ds__month",
            "listing__ds__quarter",
            "listing__ds__week",
            "listing__ds__year",
            "user__created_at__month",
            "user__created_at__quarter",
            "user__created_at__week",
            "user__created_at__year",
            "user__ds_partitioned__month",
            "user__ds_partitioned__quarter",
            "user__ds_partitioned__week",
            "user__ds_partitioned__year",
        ],
    )


def test_identifier_property(simple_model_spec_resolver: ValidLinkableSpecResolver) -> None:  # noqa: D
    property_check_helper(
        spec_resolver=simple_model_spec_resolver,
        metric_references=[MetricReference(element_name="listings")],
        element_property=LinkableElementProperties.IDENTIFIER,
        expected_names=["listing", "listing__lux_listing", "user", "user__company"],
    )
