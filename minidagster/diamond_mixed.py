from dagster import (
    AssetIn,
    AutoMaterializePolicy,
    DailyPartitionsDefinition,
    LastPartitionMapping,
    OpExecutionContext,
    asset,
)

# When partiton X of `f_first` is materialized, it will trigger the
# materialization of X for `f_second` and `f_third` and `f_fourth`
#
# When multiple partitions of `f_first` are materialized by triggering a
# backfill, only the latest partition will automatically be materialized for
# `f_second` and `f_third` and `f_fourth`


@asset(
    partitions_def=DailyPartitionsDefinition(start_date="2023-08-01"),
)
def f_first(context: OpExecutionContext) -> str:
    key = context.asset_partition_key_for_output()
    context.log.info("%s", key)
    return key


@asset(
    partitions_def=DailyPartitionsDefinition(start_date="2023-08-01"),
    auto_materialize_policy=AutoMaterializePolicy.eager(),
)
def f_second(context: OpExecutionContext, f_first: str) -> str:
    context.log.info("%s", f_first)
    return f_first * 2


@asset(
    auto_materialize_policy=AutoMaterializePolicy.eager(),
    ins={
        "val": AssetIn(
            key=f_first.key,
            partition_mapping=LastPartitionMapping(),  # Must pass a mapping
        ),
    },
)
def f_third(context: OpExecutionContext, val: str) -> str:
    context.log.info("%s", val)
    return val * 3


@asset(
    partitions_def=DailyPartitionsDefinition(start_date="2023-08-01"),
    auto_materialize_policy=AutoMaterializePolicy.eager(),
)
def f_fourth(context: OpExecutionContext, f_second: str, f_third: str) -> list[str]:
    context.log.info("%s", f_second)
    context.log.info("%s", f_third)
    return [f_second] + [f_third]
