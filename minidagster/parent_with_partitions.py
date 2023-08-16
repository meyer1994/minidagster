from dagster import *


daily = DailyPartitionsDefinition(start_date='2023-08-01')
eager = AutoMaterializePolicy.eager()


@asset(
    partitions_def=daily,
    auto_materialize_policy=eager,
)
def c_download(context: OpExecutionContext) -> str:
    key = context.asset_partition_key_for_output()
    context.log.info('Download: %s', key)
    return key


@asset(
    # deps=[c_download],  # does not trigger auto materialization
    auto_materialize_policy=eager,
    ins={
        'val': AssetIn(
            key=c_download.key,
            # input_manager_key='noop',  # we can create a noop io manager for
                                       # when we do not care about the input.
                                       # this is a HACKY workaround
            partition_mapping=LastPartitionMapping(),  # needs a mapping for 
                                                       # auto materialization
                                                       # to work
        )
    }
)
def c_parse(context: OpExecutionContext, val: Any) -> str:
    context.log.info('Parse: %s', val)
    return val * 2
