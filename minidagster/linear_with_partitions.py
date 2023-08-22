from dagster import *

# When partiton X of `a_first` is materialized, it will trigger the
# materialization of X for `a_second` and `a_third`
# 
# When multiple partitions of `a_first` are materialized by triggering a
# backfill, only the latest partition will automatically be automatically
# materialized for `a_second` and `a_third`

@asset(
    partitions_def=DailyPartitionsDefinition(start_date='2023-08-01'),
)
def a_first(context: OpExecutionContext) -> str:
    key = context.asset_partition_key_for_output()
    context.log.info('Download: %s', key)
    return key


@asset(
    partitions_def=DailyPartitionsDefinition(start_date='2023-08-01'),
    auto_materialize_policy=AutoMaterializePolicy.eager(),
)
def a_second(context: OpExecutionContext, a_first: str) -> str:
    context.log.info('Parse: %s', a_first)
    return a_first * 2


@asset(
    partitions_def=DailyPartitionsDefinition(start_date='2023-08-01'),
    auto_materialize_policy=AutoMaterializePolicy.eager(),
)
def a_third(context: OpExecutionContext, a_second: str) -> list[str]:
    context.log.info('Index: %s', a_second)
    return [a_second] * 3
