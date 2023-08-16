from dagster import *


daily = DailyPartitionsDefinition(start_date='2023-08-01')
eager = AutoMaterializePolicy.eager()


@asset(
    partitions_def=daily,
    auto_materialize_policy=eager,
)
def a_download(context: OpExecutionContext) -> str:
    key = context.asset_partition_key_for_output()
    context.log.info('Download: %s', key)
    return key


@asset(
    partitions_def=daily,
    auto_materialize_policy=eager,
)
def a_parse(context: OpExecutionContext, a_download: str) -> str:
    context.log.info('Parse: %s', a_download)
    return a_download * 2


@asset(
    partitions_def=daily,
    auto_materialize_policy=eager,
)
def a_index(context: OpExecutionContext, a_parse: str) -> list[str]:
    context.log.info('Index: %s', a_parse)
    return [a_parse] * 3
