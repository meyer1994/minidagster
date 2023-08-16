from dagster import *


daily = DailyPartitionsDefinition(start_date='2023-08-01')
eager = AutoMaterializePolicy.eager()


@asset(
    auto_materialize_policy=eager,
)
def d_download(context: OpExecutionContext) -> str:
    key = 'd_download'
    context.log.info('Download: %s', key)
    return key


@asset(
    partitions_def=daily,
    auto_materialize_policy=eager,
)
def d_parse(context: OpExecutionContext, d_download: str) -> str:
    context.log.info('Parse: %s', d_download)
    return d_download * 2
