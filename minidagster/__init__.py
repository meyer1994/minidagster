from dagster import Definitions, load_assets_from_modules, ExperimentalWarning

import warnings
warnings.filterwarnings('ignore', category=ExperimentalWarning)

from . import (
    linear_with_partitions, 
    linear_without_partitions,
    parent_with_partitions,
    parent_without_partitions,
)


linear_with_partitions = load_assets_from_modules(
    modules=[linear_with_partitions],
    key_prefix='linear_with_partitions',
    group_name='linear_with_partitions',
)

linear_without_partitions = load_assets_from_modules(
    modules=[linear_without_partitions],
    key_prefix='linear_without_partitions',
    group_name='linear_without_partitions',
)

parent_with_partitions = load_assets_from_modules(
    modules=[parent_with_partitions],
    key_prefix='parent_with_partitions',
    group_name='parent_with_partitions',
)

parent_without_partitions = load_assets_from_modules(
    modules=[parent_without_partitions],
    key_prefix='parent_without_partitions',
    group_name='parent_without_partitions',
)


assets = []
assets += linear_with_partitions
assets += linear_without_partitions
assets += parent_with_partitions
assets += parent_without_partitions


defs = Definitions(
    assets=assets
)
