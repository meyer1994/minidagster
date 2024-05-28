from typing import Any

from dagster import (
    AssetIn,
    AutoMaterializePolicy,
    DynamicPartitionsDefinition,
    OpExecutionContext,
    asset,
)

from .mappings import GroupByDownstreamSuffix, GroupByUpstreamPrefix

ids = DynamicPartitionsDefinition(name="i_ids")
prefixed_ids = DynamicPartitionsDefinition(name="i_prefixed_ids")
prefixes = DynamicPartitionsDefinition(name="i_prefixes")


FAKE_DATA = {
    "1": "a",
    "2": "a",
    "3": "a",
    "4": "b",
    "5": "b",
    "6": "c",
}


@asset(
    auto_materialize_policy=AutoMaterializePolicy.eager(),
)
def i_zero(context: OpExecutionContext) -> None:
    """Dummy asset to create partitions for testing"""
    partitions = list(FAKE_DATA)
    assert ids.name
    context.instance.add_dynamic_partitions(ids.name, partitions)


@asset(
    deps=[i_zero],
    partitions_def=ids,
    auto_materialize_policy=AutoMaterializePolicy.eager(),
)
def i_first(context: OpExecutionContext) -> str:
    context.log.info("%s", context.partition_key)
    return context.partition_key


@asset(
    partitions_def=ids,
    auto_materialize_policy=AutoMaterializePolicy.eager(),
)
def i_second(context: OpExecutionContext, i_first: str) -> str:
    context.log.info("%s", context.partition_key)

    # Create dynamic partitions based on the data we are processing
    prefix = FAKE_DATA[context.partition_key]
    partition = f"{prefix}_{context.partition_key}"

    # typing
    assert prefixed_ids.name
    assert prefixes.name

    context.instance.add_dynamic_partitions(prefixed_ids.name, [partition])
    context.instance.add_dynamic_partitions(prefixes.name, [prefix])

    return context.partition_key


@asset(
    partitions_def=prefixed_ids,
    auto_materialize_policy=AutoMaterializePolicy.eager(),
    ins={
        "val": AssetIn(
            key=i_second.key,
            partition_mapping=GroupByDownstreamSuffix("_", i_second.key),
        ),
    },
)
def i_third(context: OpExecutionContext, val: Any) -> str:
    context.log.info("%s", val)
    context.log.info("%s", context.partition_key)
    return context.partition_key


@asset(
    partitions_def=prefixes,
    auto_materialize_policy=AutoMaterializePolicy.eager(),
    ins={
        "val": AssetIn(
            key=i_third.key, partition_mapping=GroupByUpstreamPrefix("_", i_third.key)
        ),
    },
)
def i_fourth(context: OpExecutionContext, val: Any) -> str:
    context.log.info("%s", val)
    context.log.info("%s", context.partition_key)
    return context.partition_key
