from datetime import datetime
from collections import defaultdict

from dagster import *
from dagster._core.definitions.partition import (
    PartitionsSubset,
    DynamicPartitionsDefinition,
    DefaultPartitionsSubset
)
from dagster._core.definitions.partition_mapping import UpstreamPartitionsResult
from dagster._core.instance import DynamicPartitionsStore


class GroupByUpstreamPrefix(PartitionMapping):
    def __init__(self, separator: str, upstream_key: AssetKey):
        super(GroupByUpstreamPrefix, self).__init__()
        self.separator = separator
        self.upstream_key = upstream_key

    def get_downstream_partitions_for_partitions(
        self,
        upstream_partitions_subset: PartitionsSubset,
        downstream_partitions_def: DynamicPartitionsDefinition,
        current_time: Optional[datetime] = None,
        dynamic_partitions_store: Optional[DynamicPartitionsStore] = None
    ) -> PartitionsSubset:
        raise NotImplementedError()

    def get_upstream_mapped_partitions_result_for_partitions(
        self,
        downstream_partitions_subset: Optional[PartitionsSubset],
        upstream_partitions_def: DynamicPartitionsDefinition,
        current_time: Optional[datetime] = None,
        dynamic_partitions_store: Optional[DynamicPartitionsStore] = None
    ) -> UpstreamPartitionsResult:
        # First, we get all partitions for the dynamic partiton definition
        partitions = dynamic_partitions_store.get_dynamic_partitions(
            partitions_def_name=upstream_partitions_def.name
        )

        # Second, we get the status of all of those partitions
        status = dynamic_partitions_store.get_status_by_partition(
            asset_key=self.upstream_key,
            partition_keys=partitions,
            partitions_def=upstream_partitions_def,
        )

        # Third, we filter to have the ones that have been materialized
        materialized = set()
        for k, v in status.items():
            if v == AssetPartitionStatus.MATERIALIZED:
                materialized.add(k)

        # Fourth, we create a table of the prefix part of the name to all
        # partitions that had the same prefix part
        table = defaultdict(set)
        for i in materialized:
            prefix, *_ = i.split(self.separator)
            table[prefix].add(i)

        # Fifth, we filter out the partitions to send downstream
        downstream = set()
        for k, v in table.items():
            if k in downstream_partitions_subset:
                downstream |= v

        subset = DefaultPartitionsSubset(upstream_partitions_def, downstream)
        return UpstreamPartitionsResult(subset, set())



class GroupByDownstreamSuffix(PartitionMapping):
    def __init__(self, separator: str, upstream_key: AssetKey):
        super(GroupByDownstreamSuffix, self).__init__()
        self.separator = separator
        self.upstream_key = upstream_key

    def get_downstream_partitions_for_partitions(
        self,
        upstream_partitions_subset: PartitionsSubset,
        downstream_partitions_def: DynamicPartitionsDefinition,
        current_time: Optional[datetime] = None,
        dynamic_partitions_store: Optional[DynamicPartitionsStore] = None
    ) -> PartitionsSubset:
        raise NotImplementedError()

    def get_upstream_mapped_partitions_result_for_partitions(
        self,
        downstream_partitions_subset: Optional[PartitionsSubset],
        upstream_partitions_def: DynamicPartitionsDefinition,
        current_time: Optional[datetime] = None,
        dynamic_partitions_store: Optional[DynamicPartitionsStore] = None
    ) -> UpstreamPartitionsResult:
        downstream = set()
        for i in downstream_partitions_subset.get_partition_keys():
            *_, suffix = i.split(self.separator)
            downstream.add(suffix)

        subset = DefaultPartitionsSubset(upstream_partitions_def, downstream)
        return UpstreamPartitionsResult(subset, set())
