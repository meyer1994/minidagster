from collections import defaultdict

from dagster import *

from .mappings import GroupByUpstreamPrefix


prefixed = DynamicPartitionsDefinition(name='h_prefixed')
grouped = DynamicPartitionsDefinition(name='h_grouped')


@asset(
    auto_materialize_policy=AutoMaterializePolicy.eager(),
)
def h_zero(context: OpExecutionContext) -> None:
    keys = ['a_1', 'a_2', 'a_3', 'b_1', 'b_2', 'c_1']
    context.instance.add_dynamic_partitions(prefixed.name, keys)


@asset(
    deps=[h_zero],
    partitions_def=prefixed,
    auto_materialize_policy=AutoMaterializePolicy.eager(),
)
def h_first(context: OpExecutionContext) -> str:
    context.log.info("%s", context.partition_key)
    key, *_ = context.partition_key.split('_')
    context.instance.add_dynamic_partitions(grouped.name, [key])
    return context.partition_key


@asset(
    partitions_def=grouped,
    ins={
        'val': AssetIn(
            key=h_first.key,
            partition_mapping=GroupByUpstreamPrefix('_', h_first.key),
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
