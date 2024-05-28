from dagster import (
    AssetIn,
    AutoMaterializePolicy,
    DynamicPartitionsDefinition,
    MultiPartitionsDefinition,
    MultiToSingleDimensionPartitionMapping,
    OpExecutionContext,
    SensorResult,
    asset,
    sensor,
)

docs = DynamicPartitionsDefinition(name="docs")
grouped = DynamicPartitionsDefinition(name="grouped")
partition = MultiPartitionsDefinition({"grouped": grouped, "docs": docs})


@sensor()
def random_docs_sensor():
    """Dummy sensor to create random partitions"""
    keys = [f"{i:03d}" for i in range(10)]
    requests = docs.build_add_request(keys)
    return SensorResult(dynamic_partitions_requests=[requests])


@sensor()
def random_grouped_sensor():
    """Dummy sensor to create random partitions"""
    keys = ["a", "b", "c"]
    requests = grouped.build_add_request(keys)
    return SensorResult(dynamic_partitions_requests=[requests])


@asset(
    partitions_def=partition,
)
def g_first(context: OpExecutionContext) -> str:
    context.log.info("%s", context.partition_key)
    return context.partition_key


@asset(
    partitions_def=grouped,
    ins={
        "val": AssetIn(
            key=g_first.key,
            partition_mapping=MultiToSingleDimensionPartitionMapping("grouped"),
        ),
    },
    auto_materialize_policy=AutoMaterializePolicy.eager(),
)
def g_second(context: OpExecutionContext, val: dict) -> dict:
    context.log.info("%s", val)
    return val
