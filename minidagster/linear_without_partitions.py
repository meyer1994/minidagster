from dagster import AutoMaterializePolicy, OpExecutionContext, asset

# When `b_first` is materialized, it will materialize `b_second` and
# `b_third`


@asset
def b_first(context: OpExecutionContext) -> str:
    key = "download"
    context.log.info("%s", key)
    return key


@asset(
    auto_materialize_policy=AutoMaterializePolicy.eager(),
)
def b_second(context: OpExecutionContext, b_first: str) -> str:
    context.log.info("%s", b_first)
    return b_first * 2


@asset(
    auto_materialize_policy=AutoMaterializePolicy.eager(),
)
def b_third(context: OpExecutionContext, b_second: str) -> list[str]:
    context.log.info("%s", b_second)
    return [b_second] * 3
