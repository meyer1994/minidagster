from dagster import Definitions, load_assets_from_modules, ExperimentalWarning

import warnings
warnings.filterwarnings('ignore', category=ExperimentalWarning)

from . import (
    linear_with_partitions, 
    linear_without_partitions,
    mapping_multi_dynamic_partitions,
    mapping_multi_dynamic_partitions_complex,
    parent_with_partitions,
    parent_without_partitions,
    diamond_with_partitions,
    diamond_mixed,
    resources,
)


linear_with_partitions_assets = load_assets_from_modules(
    modules=[linear_with_partitions],
    group_name='linear_with_partitions',
)

linear_without_partitions_assets = load_assets_from_modules(
    modules=[linear_without_partitions],
    group_name='linear_without_partitions',
)

parent_with_partitions_assets = load_assets_from_modules(
    modules=[parent_with_partitions],
    group_name='parent_with_partitions',
)

parent_without_partitions_assets = load_assets_from_modules(
    modules=[parent_without_partitions],
    group_name='parent_without_partitions',
)

diamond_with_partitions_assets = load_assets_from_modules(
    modules=[diamond_with_partitions],
    group_name='diamond_with_partitions',
)

diamond_mixed_assets = load_assets_from_modules(
    modules=[diamond_mixed],
    group_name='diamond_mixed',
)

mapping_multi_dynamic_partitions_assets = load_assets_from_modules(
    modules=[mapping_multi_dynamic_partitions],
    group_name='mapping_multi_dynamic_partitions',
)

mapping_multi_dynamic_partitions_complex_assets = load_assets_from_modules(
    modules=[mapping_multi_dynamic_partitions_complex],
    group_name='mapping_multi_dynamic_partitions_complex',
)


assets = []
assets += linear_with_partitions_assets
assets += linear_without_partitions_assets
assets += parent_with_partitions_assets
assets += parent_without_partitions_assets
assets += diamond_with_partitions_assets
assets += diamond_mixed_assets
assets += mapping_multi_dynamic_partitions_assets
assets += mapping_multi_dynamic_partitions_complex_assets


defs = Definitions(
    assets=assets,
    sensors=[
        mapping_multi_dynamic_partitions.random_docs_sensor,
        mapping_multi_dynamic_partitions.random_grouped_sensor,
        mapping_multi_dynamic_partitions_complex.h_first_sensor,
    ],
    resources={
        'noop': resources.noop,
    },
)
