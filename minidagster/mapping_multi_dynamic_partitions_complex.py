import datetime as dt
from typing import Optional, Union
import itertools
from collections import defaultdict

from dagster import *
from dagster._core.definitions.partition import (
    PartitionsDefinition,
    PartitionsSubset,
    DynamicPartitionsDefinition,
    DefaultPartitionsSubset
)
from dagster._core.definitions.partition_mapping import UpstreamPartitionsResult
from dagster._core.instance import DynamicPartitionsStore

docs = DynamicPartitionsDefinition(name='complex_docs')
grouped = DynamicPartitionsDefinition(name='complex_grouped')

multi = MultiPartitionsDefinition({'grouped': grouped, 'docs': docs})


@asset(
    auto_materialize_policy=AutoMaterializePolicy.eager(),
)
def h_zero(context: OpExecutionContext) -> None:
    keys = ['a_1', 'a_2', 'a_3', 'b_1', 'b_2', 'c_1']
    context.instance.add_dynamic_partitions('complex_docs', keys)


@asset(
    deps=[h_zero],
    partitions_def=docs,
    auto_materialize_policy=AutoMaterializePolicy.eager(),
)
def h_first(context: OpExecutionContext) -> str:
    context.log.info("%s", context.partition_key)
    key, *_ = context.partition_key.split('_')
    context.instance.add_dynamic_partitions('complex_grouped', [key])
    return context.partition_key


class DocToCnpjPartitionMapping(PartitionMapping):
    def get_downstream_partitions_for_partitions(
        self, 
        upstream_partitions_subset: PartitionsSubset, 
        downstream_partitions_def: PartitionsDefinition, 
        current_time: Optional[dt.datetime] = None, 
        dynamic_partitions_store: Optional[DynamicPartitionsStore] = None
    ) -> PartitionsSubset:
        raise NotImplementedError()
        
    def get_upstream_mapped_partitions_result_for_partitions(
        self, 
        downstream_partitions_subset: Optional[PartitionsSubset], 
        upstream_partitions_def: PartitionsDefinition, 
        current_time: Optional[dt.datetime] = None, 
        dynamic_partitions_store: Optional[DynamicPartitionsStore] = None
    ) -> UpstreamPartitionsResult:
        ups = dynamic_partitions_store.get_dynamic_partitions('complex_docs')
        
        stats = dynamic_partitions_store.get_status_by_partition(
            h_first.key,
            ups,
            docs,
        )

        materialized = {k for k, v in stats.items() if v == AssetPartitionStatus.MATERIALIZED}
        ups = [i.split('_') for i in materialized]

        table = defaultdict(set)
        for first, second in ups:
            table[first] |= {f'{first}_{second}'}

        downs = {tuple(v) for k, v in table.items() if k in downstream_partitions_subset}
        downs = itertools.chain.from_iterable(downs)
        downs = {i for i in downs if i in materialized}
        downs = DefaultPartitionsSubset(docs, downs)

        return UpstreamPartitionsResult(downs, set())
        

@asset(
    partitions_def=grouped,
    ins={
        'val': AssetIn(
            key=h_first.key,
            partition_mapping=DocToCnpjPartitionMapping(),
        ),
    },
    auto_materialize_policy=AutoMaterializePolicy.eager(),
)
def h_second(context: OpExecutionContext, val: Any) -> dict:
    context.log.info("%s", val)
    return dict()


@multi_asset_sensor(
    monitored_assets=[h_first.key],
    job=define_asset_job('sample', selection=AssetSelection.assets(h_second))
)
def h_first_sensor(context: MultiAssetSensorEvaluationContext):
    records = context.latest_materialization_records_by_partition(h_first.key)

    partitions = defaultdict(set)
    for i in records:
        key, part = i.split('_')
        partitions[key].add(f'{key}_{part}')

    context.advance_all_cursors()

    return [RunRequest(partition_key=i) for i in partitions]
