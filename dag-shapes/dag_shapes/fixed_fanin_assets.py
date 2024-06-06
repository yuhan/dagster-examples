from typing import List

from dagster import graph, op, Definitions, graph_asset


@op
def return_one() -> int:
    return 1


@op
def sum_fan_in(nums: List[int]) -> int:
    return sum(nums)


# @graph
# def fan_in():
#     fan_outs = []
#     for i in range(0, 10):
#         fan_outs.append(return_one.alias(f"return_one_{i}")())
#     sum_fan_in(fan_outs)


@graph_asset
def fan_in():
    fan_outs = []
    for i in range(0, 10):
        fan_outs.append(return_one.alias(f"return_one_{i}")())
    return sum_fan_in(fan_outs)

# Example of how to define the assets in a job
from dagster import Definitions, define_asset_job

defs = Definitions(
    assets=[fan_in],
)
