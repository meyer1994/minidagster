from dagster import *

# When any partition of `c_first` is materialized, it will trigger `c_second`

@asset(
    partitions_def=DailyPartitionsDefinition(start_date='2023-08-01'),
)
def c_first(context: OpExecutionContext) -> str:
    key = context.asset_partition_key_for_output()
    context.log.info('%s', key)
    return key


@asset(
    # deps=[c_first],  # does not work, maybe a bug?
    auto_materialize_policy=AutoMaterializePolicy.eager(),
    ins={
        'val': AssetIn(
            key=c_first.key,
            partition_mapping=LastPartitionMapping(),  # Must pass a mapping
        ),
    },
)
def c_second(context: OpExecutionContext, val: Any) -> str:
    context.log.info('%s', val)
    return val * 2


# The example below shows a way that you can define dependencies without having
# the data loaded. We do this by creating a dummy, noop, IO manager. This way,
# dagster will, in fact, try to load the asset, but as the IO manager does
# nothing, it will load nothing...
#
# The above behaviour _should_ be working by using the `deps` paramter of 
# `@asset`. However, this does not work. Not sure why...
#
# @asset(
#     auto_materialize_policy=AutoMaterializePolicy.eager(),
#     ins={
#         'val': AssetIn(
#             key=c_first.key,
#             partition_mapping=LastPartitionMapping(),
#             input_manager_key='noop'
#         ),
#     },
# )
# def c_second2(context: OpExecutionContext, val: Any) -> str:
#     context.log.info('%s', val)
#     return val * 2
