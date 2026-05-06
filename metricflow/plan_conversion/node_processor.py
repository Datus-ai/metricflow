import logging
from dataclasses import dataclass
from typing import Generic, Sequence, List, TypeVar, Optional, Set, Tuple

from metricflow.constraints.time_constraint import TimeRangeConstraint
from metricflow.dataflow.builder.node_data_set import DataflowPlanNodeOutputDataSetResolver
from metricflow.dataflow.builder.partitions import PartitionJoinResolver
from metricflow.dataflow.dataflow_plan import (
    ConstrainTimeRangeNode,
    BaseOutput,
    JoinToBaseOutputNode,
    FilterElementsNode,
    JoinDescription,
)
from metricflow.model.semantics.data_source_join_evaluator import DataSourceJoinEvaluator
from metricflow.object_utils import pformat_big_objects
from metricflow.plan_conversion.sql_dataset import SqlDataSet
from metricflow.protocols.semantics import DataSourceSemanticsAccessor
from metricflow.references import TimeDimensionReference, IdentifierReference
from metricflow.spec_set_transforms import ToElementNameSet
from metricflow.specs import LinkableInstanceSpec, LinklessIdentifierSpec, InstanceSpecSet

SqlDataSetT = TypeVar("SqlDataSetT", bound=SqlDataSet)


logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class MultiHopJoinCandidateLineage(Generic[SqlDataSetT]):
    """Describes how the multi-hop join candidate was formed.

    For a 2-hop join (account_id__customer_id__customer_name):
    * bridge_source has the primary identifier account_id and the foreign identifier customer_id
    * customers_source has the primary identifier customer_id and dimension customer_name

    Lineage: source_nodes_in_chain = (bridge_source, customers_source)
             join_by_identifiers = (LinklessIdentifierSpec("customer_id"),)

    For a 3-hop join (a__b__c__dim):
    * bridge1 has identifiers a and b
    * bridge2 has identifiers b and c
    * target has identifier c and dimension dim

    Lineage: source_nodes_in_chain = (bridge1, bridge2, target)
             join_by_identifiers = (LinklessIdentifierSpec("c"), LinklessIdentifierSpec("b"))
    """

    source_nodes_in_chain: Tuple[BaseOutput[SqlDataSetT], ...]
    join_by_identifiers: Tuple[LinklessIdentifierSpec, ...]


@dataclass(frozen=True)
class MultiHopJoinCandidate(Generic[SqlDataSetT]):
    """A candidate node containing linkable specs that is join of other nodes. It's used to resolve multi-hop queries.

    Also see MultiHopJoinCandidateLineage.
    """

    node_with_multi_hop_elements: BaseOutput[SqlDataSetT]
    lineage: MultiHopJoinCandidateLineage[SqlDataSetT]


class PreDimensionJoinNodeProcessor(Generic[SqlDataSetT]):
    """Processes source nodes before measures are joined to dimensions.

    Generally, the source nodes will be combined with other dataflow plan nodes to produce a new set of nodes to realize
    a condition of the query. For example, to realize a time range constraint, a ConstrainTimeRangeNode will be added
    to the source nodes.

    e.g.

    <SomeDataflowPlanNode/>

    ->

    <ConstrainTimeRangeNode>
        <SomeDataflowPlanNode/>
    </ConstrainTimeRangeNode>

    """

    def __init__(  # noqa: D
        self,
        data_source_semantics: DataSourceSemanticsAccessor,
        node_data_set_resolver: DataflowPlanNodeOutputDataSetResolver[SqlDataSetT],
    ):
        self._node_data_set_resolver = node_data_set_resolver
        self._partition_resolver = PartitionJoinResolver(data_source_semantics)
        self._data_source_semantics = data_source_semantics
        self._join_evaluator = DataSourceJoinEvaluator(data_source_semantics)

    def add_time_range_constraint(
        self,
        source_nodes: Sequence[BaseOutput[SqlDataSetT]],
        metric_time_dimension_reference: TimeDimensionReference,
        time_range_constraint: Optional[TimeRangeConstraint] = None,
    ) -> Sequence[BaseOutput[SqlDataSetT]]:
        """Adds a time range constraint node to the input nodes."""
        processed_nodes: List[BaseOutput[SqlDataSetT]] = []
        for source_node in source_nodes:

            # Constrain the time range if specified.
            if time_range_constraint:
                node_output_data_set = self._node_data_set_resolver.get_output_data_set(source_node)
                constrain_time = False
                for time_dimension_instance in node_output_data_set.instance_set.time_dimension_instances:
                    if (
                        time_dimension_instance.spec.reference == metric_time_dimension_reference
                        and len(time_dimension_instance.spec.identifier_links) == 0
                    ):
                        constrain_time = True
                        break
                if constrain_time:
                    processed_nodes.append(
                        ConstrainTimeRangeNode(parent_node=source_node, time_range_constraint=time_range_constraint)
                    )
                else:
                    processed_nodes.append(source_node)
            else:
                processed_nodes.append(source_node)
        return processed_nodes

    def _node_contains_identifier(
        self,
        node: BaseOutput[SqlDataSetT],
        identifier_reference: IdentifierReference,
    ) -> bool:
        """Returns true if the output of the node contains an identifier of the given types."""
        data_set = self._node_data_set_resolver.get_output_data_set(node)

        for identifier_instance_in_first_node in data_set.instance_set.identifier_instances:
            identifier_spec_in_first_node = identifier_instance_in_first_node.spec

            if identifier_spec_in_first_node.reference != identifier_reference:
                continue

            if len(identifier_spec_in_first_node.identifier_links) > 0:
                continue

            assert (
                len(identifier_instance_in_first_node.defined_from) == 1
            ), "Multiple items in defined_from not yet supported"

            identifier = self._data_source_semantics.get_identifier_in_data_source(
                identifier_instance_in_first_node.defined_from[0]
            )
            if identifier is None:
                raise RuntimeError(
                    f"Invalid DataSourceElementReference {identifier_instance_in_first_node.defined_from[0]}"
                )

            return True

        return False

    def _nodes_contain_multiple_validity_windows(self, nodes: Sequence[BaseOutput[SqlDataSetT]]) -> bool:
        """Return true if the nodes include more than one source with validity-window dimensions."""

        data_sources_with_validity_windows: Set[str] = set()
        for node in nodes:
            instance_set = self._node_data_set_resolver.get_output_data_set(node).instance_set
            data_source_element_instances = (
                instance_set.measure_instances
                + instance_set.dimension_instances
                + instance_set.time_dimension_instances
                + instance_set.identifier_instances
            )
            for instance in data_source_element_instances:
                for defined_from in instance.defined_from:
                    data_source = self._data_source_semantics.get_by_reference(defined_from.data_source_reference)
                    if data_source and data_source.has_validity_dimensions:
                        data_sources_with_validity_windows.add(data_source.name)
                        if len(data_sources_with_validity_windows) > 1:
                            return True

        return False

    def _get_candidates_nodes_for_multi_hop(
        self,
        desired_linkable_spec: LinkableInstanceSpec,
        nodes: Sequence[BaseOutput[SqlDataSetT]],
    ) -> Sequence[MultiHopJoinCandidate]:
        """Assemble nodes representing all possible multi-hop join paths.

        For a desired linkable spec with N identifier links (N >= 2), this builds
        composite nodes by iteratively joining source nodes from right to left.

        For identifier_links = [l0, l1, ..., l_{n-1}] and target element e:
        1. Find source nodes containing l_{n-1} and element e (leaf nodes)
        2. Find bridge nodes with l_{n-2} and l_{n-1}, join with leaf nodes on l_{n-1}
        3. Continue until the composite has l0 and can be joined to the measure node.
        """
        n_links = len(desired_linkable_spec.identifier_links)
        if n_links < 2:
            return ()

        multi_hop_join_candidates: List[MultiHopJoinCandidate] = []
        logger.info(f"Creating multi-hop nodes for {desired_linkable_spec} ({n_links} hops)")

        last_link_ref = desired_linkable_spec.identifier_links[-1]

        # Step 0: Find initial leaf nodes containing the last identifier and target element.
        # Each candidate is (node, source_nodes_used, join_identifiers_used).
        current_candidates: List[
            Tuple[
                BaseOutput[SqlDataSetT],
                Tuple[BaseOutput[SqlDataSetT], ...],
                Tuple[LinklessIdentifierSpec, ...],
            ]
        ] = []

        for node in nodes:
            if not self._node_contains_identifier(node=node, identifier_reference=last_link_ref):
                continue

            data_set = self._node_data_set_resolver.get_output_data_set(node)
            element_names_in_data_set = ToElementNameSet().transform(data_set.instance_set.spec_set)
            if desired_linkable_spec.element_name not in element_names_in_data_set:
                continue

            current_candidates.append((node, (node,), ()))

        # Iteratively build composites from right to left.
        # At each step, we find bridge nodes that connect the current chain one hop further left.
        for step in range(n_links - 2, -1, -1):
            bridge_link_ref = desired_linkable_spec.identifier_links[step]
            join_link_ref = desired_linkable_spec.identifier_links[step + 1]
            join_link_spec = LinklessIdentifierSpec.from_reference(join_link_ref)

            next_candidates: List[
                Tuple[
                    BaseOutput[SqlDataSetT],
                    Tuple[BaseOutput[SqlDataSetT], ...],
                    Tuple[LinklessIdentifierSpec, ...],
                ]
            ] = []

            for bridge_node in nodes:
                # Bridge node must have both the current link and the next link.
                if not (
                    self._node_contains_identifier(node=bridge_node, identifier_reference=bridge_link_ref)
                    and self._node_contains_identifier(node=bridge_node, identifier_reference=join_link_ref)
                ):
                    continue

                for right_node, used_source_nodes, join_identifiers in current_candidates:
                    # Avoid cycles: don't reuse the same source node.
                    if bridge_node.node_id == right_node.node_id or any(
                        bridge_node.node_id == used.node_id for used in used_source_nodes
                    ):
                        continue

                    source_nodes_in_chain = (bridge_node,) + used_source_nodes
                    if self._nodes_contain_multiple_validity_windows(source_nodes_in_chain):
                        continue

                    data_set_of_bridge = self._node_data_set_resolver.get_output_data_set(bridge_node)
                    data_set_of_right = self._node_data_set_resolver.get_output_data_set(right_node)

                    if not self._join_evaluator.is_valid_instance_set_join(
                        left_instance_set=data_set_of_bridge.instance_set,
                        right_instance_set=data_set_of_right.instance_set,
                        on_identifier_reference=join_link_ref,
                    ):
                        continue

                    # Filter right node to only keep linkable specs (exclude measures).
                    specs = data_set_of_right.instance_set.spec_set
                    filtered_right_node = FilterElementsNode(
                        parent_node=right_node,
                        include_specs=InstanceSpecSet.create_from_linkable_specs(
                            specs.dimension_specs + specs.identifier_specs + specs.time_dimension_specs
                        ),
                    )

                    join_on_partition_dimensions = self._partition_resolver.resolve_partition_dimension_joins(
                        start_node_spec_set=data_set_of_bridge.instance_set.spec_set,
                        node_to_join_spec_set=data_set_of_right.instance_set.spec_set,
                    )
                    join_on_partition_time_dimensions = self._partition_resolver.resolve_partition_time_dimension_joins(
                        start_node_spec_set=data_set_of_bridge.instance_set.spec_set,
                        node_to_join_spec_set=data_set_of_right.instance_set.spec_set,
                    )

                    composite = JoinToBaseOutputNode(
                        left_node=bridge_node,
                        join_targets=[
                            JoinDescription(
                                join_node=filtered_right_node,
                                join_on_identifier=join_link_spec,
                                join_on_partition_dimensions=join_on_partition_dimensions,
                                join_on_partition_time_dimensions=join_on_partition_time_dimensions,
                            )
                        ],
                    )

                    next_candidates.append(
                        (
                            composite,
                            source_nodes_in_chain,
                            (join_link_spec,) + join_identifiers,
                        )
                    )

            current_candidates = next_candidates

        # Build final MultiHopJoinCandidate objects from the fully-assembled composites.
        for composite, source_nodes, join_identifiers in current_candidates:
            multi_hop_join_candidates.append(
                MultiHopJoinCandidate(
                    node_with_multi_hop_elements=composite,
                    lineage=MultiHopJoinCandidateLineage(
                        source_nodes_in_chain=source_nodes,
                        join_by_identifiers=join_identifiers,
                    ),
                )
            )

        for multi_hop_join_candidate in multi_hop_join_candidates:
            output_data_set = self._node_data_set_resolver.get_output_data_set(
                multi_hop_join_candidate.node_with_multi_hop_elements
            )
            logger.debug(
                f"Node {multi_hop_join_candidate.node_with_multi_hop_elements} has spec set:\n"
                f"{pformat_big_objects(output_data_set.instance_set.spec_set)}"
            )

        return multi_hop_join_candidates

    def add_multi_hop_joins(
        self, desired_linkable_specs: Sequence[LinkableInstanceSpec], nodes: Sequence[BaseOutput[SqlDataSetT]]
    ) -> Sequence[BaseOutput[SqlDataSetT]]:
        """Assemble nodes representing all possible one-hop joins"""

        all_multi_hop_join_candidates: List[MultiHopJoinCandidate[SqlDataSetT]] = []
        lineage_for_all_multi_hop_join_candidates: Set[MultiHopJoinCandidateLineage[SqlDataSetT]] = set()

        for desired_linkable_spec in desired_linkable_specs:
            for multi_hop_join_candidate in self._get_candidates_nodes_for_multi_hop(
                desired_linkable_spec=desired_linkable_spec,
                nodes=nodes,
            ):
                # Dedupe candidates that are the same join.
                if multi_hop_join_candidate.lineage not in lineage_for_all_multi_hop_join_candidates:
                    all_multi_hop_join_candidates.append(multi_hop_join_candidate)
                    lineage_for_all_multi_hop_join_candidates.add(multi_hop_join_candidate.lineage)

        return list(x.node_with_multi_hop_elements for x in all_multi_hop_join_candidates) + list(nodes)

    def remove_unnecessary_nodes(
        self,
        desired_linkable_specs: Sequence[LinkableInstanceSpec],
        nodes: Sequence[BaseOutput[SqlDataSetT]],
        metric_time_dimension_reference: TimeDimensionReference,
    ) -> Sequence[BaseOutput[SqlDataSetT]]:
        """Filters out many of the nodes that can't possibly be useful for joins to obtain the desired linkable specs.

        A simple filter is to remove any nodes that don't share a common element with the query. Having a common element
        doesn't mean that the node will be useful, but not having common elements definitely means it's not useful.
        """
        relevant_element_names = {x.element_name for x in desired_linkable_specs}.union(
            {y.element_name for x in desired_linkable_specs for y in x.identifier_links}
        )

        # The metric time dimension is used everywhere, so don't count it unless specifically desired in linkable spec
        # that has identifier links.
        metric_time_dimension_used_in_linked_spec = any(
            [
                len(linkable_spec.identifier_links) > 0
                and linkable_spec.element_name == metric_time_dimension_reference.element_name
                for linkable_spec in desired_linkable_specs
            ]
        )

        if (
            metric_time_dimension_reference.element_name in relevant_element_names
            and not metric_time_dimension_used_in_linked_spec
        ):
            relevant_element_names.remove(metric_time_dimension_reference.element_name)

        logger.info(f"Relevant names are: {relevant_element_names}")

        relevant_nodes = []

        for node in nodes:
            data_set = self._node_data_set_resolver.get_output_data_set(node)
            element_names_in_data_set = ToElementNameSet().transform(data_set.instance_set.spec_set)

            if len(element_names_in_data_set.intersection(relevant_element_names)) > 0:
                relevant_nodes.append(node)

        return relevant_nodes
