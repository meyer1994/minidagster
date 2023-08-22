from dagster import *

# When partiton X of `e_first` is materialized, it will trigger the
# materialization of X for `e_second` and `e_third`
# 
# When multiple partitions of `e_first` are materialized by triggering a
# backfill, only the latest partition will automatically be automatically
# materialized for `e_second` and `e_third`

@asset(
    partitions_def=DailyPartitionsDefinition(start_date='2023-08-01'),
)
def e_first(context: OpExecutionContext) -> str:
    key = context.asset_partition_key_for_output()
    context.log.info('Download: %s', key)
    return key


@asset(
    partitions_def=DailyPartitionsDefinition(start_date='2023-08-01'),
    auto_materialize_policy=AutoMaterializePolicy.eager(),
)
def e_second(context: OpExecutionContext, e_first: str) -> str:
    context.log.info('Parse: %s', e_first)
    return e_first * 2


@asset(
    partitions_def=DailyPartitionsDefinition(start_date='2023-08-01'),
    auto_materialize_policy=AutoMaterializePolicy.eager(),
)
def e_third(context: OpExecutionContext, e_second: str) -> list[str]:
    context.log.info('Index: %s', e_second)
    return [e_second] * 3


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
    context.log.info('Index: %s', e_second)
    return [e_second] * 3
