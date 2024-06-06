from dagster import Definitions, load_assets_from_modules

from . import fixed_fanin_assets

all_fixed_fanin_assets = load_assets_from_modules([fixed_fanin_assets])

defs = Definitions(
    assets=all_fixed_fanin_assets,
)
