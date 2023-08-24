from dagster import *

# When partiton X of `e_first` is materialized, it will trigger the
# materialization of X for `e_second` and `e_third` and `e_fourth`
# 
# When multiple partitions of `e_first` are materialized by triggering a
# backfill, only the latest partition will automatically be materialized for 
# `e_second` and `e_third` and `e_fourth`
#
# As all assets have the same partition, we do not need to add much
# configuration. By default, dagster sees that they all have the same partition
# and will map each partiton 1 to 1

@asset(
    partitions_def=DailyPartitionsDefinition(start_date='2023-08-01'),
)
def e_first(context: OpExecutionContext) -> str:
    key = context.asset_partition_key_for_output()
    context.log.info('%s', key)
    return key


@asset(
    partitions_def=DailyPartitionsDefinition(start_date='2023-08-01'),
    auto_materialize_policy=AutoMaterializePolicy.eager(),
)
def e_second(context: OpExecutionContext, e_first: str) -> str:
    context.log.info('%s', e_first)
    return e_first * 2


@asset(
    partitions_def=DailyPartitionsDefinition(start_date='2023-08-01'),
    auto_materialize_policy=AutoMaterializePolicy.eager(),
)
def e_third(context: OpExecutionContext, e_first: str) -> str:
    context.log.info('%s', e_first)
    return e_first * 3


@asset(
    partitions_def=DailyPartitionsDefinition(start_date='2023-08-01'),
    auto_materialize_policy=AutoMaterializePolicy.eager(),
)
def e_fourth(
    context: 
    OpExecutionContext, 
    e_second: str,
    e_third: str
) -> list[str]:
    context.log.info('%s', e_second)
    context.log.info('%s', e_third)
    return [e_second] * 4
