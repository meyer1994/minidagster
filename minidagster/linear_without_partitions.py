from dagster import *


eager = AutoMaterializePolicy.eager()


@asset(
    auto_materialize_policy=eager,
)
def b_download(context: OpExecutionContext) -> str:
    key = 'download'
    context.log.info('Download: %s', key)
    return key


@asset(
    auto_materialize_policy=eager,
)
def b_parse(context: OpExecutionContext, b_download: str) -> str:
    context.log.info('Parse: %s', b_download)
    return b_download * 2


@asset(
    auto_materialize_policy=eager,
)
def b_index(context: OpExecutionContext, b_parse: str) -> list[str]:
    context.log.info('Index: %s', b_parse)
    return [b_parse] * 3
