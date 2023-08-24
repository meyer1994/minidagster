from dagster import *

# When `d_first` gets materialized, it will trigger materialization of the
# latest partition of `d_second`

@asset
def d_first(context: OpExecutionContext) -> str:
    key = 'd_first'
    context.log.info('%s', key)
    return key


@asset(
    partitions_def=DailyPartitionsDefinition(start_date='2023-08-01'),
    auto_materialize_policy=AutoMaterializePolicy.eager(),
)
def d_second(context: OpExecutionContext, d_first: str) -> str:
    context.log.info('%s', d_first)
    return d_first * 2
