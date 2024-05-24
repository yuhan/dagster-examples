from typing import Generator, Tuple

import polars as pl
from dagster import (
    Definitions,
    DynamicOut,
    DynamicOutput,
    graph_asset,
    op,
)


#### dummy functions for repro purposes
def load_filenames() -> list[Tuple[str, str]]:
    return [(str(i), f"file_{i}.csv") for i in range(10)]

def some_function_that_does_that(filename: str) -> pl.LazyFrame:
    return pl.LazyFrame({"a": [1, 2, 3], "b": [4, 5, 6]})


@op(out=DynamicOut(str))
def load_pieces() -> Generator[DynamicOut, None, None]:
    filenames = load_filenames()
    for idx, fn in filenames:
        yield DynamicOutput(fn, mapping_key=idx)

@op
def download_and_transform(filename: str) -> pl.LazyFrame:
    return some_function_that_does_that(filename)

@op
def merge(dfs: list[pl.LazyFrame]) -> pl.LazyFrame:
    return pl.concat(dfs)

@graph_asset
def dynamic_graph_asset():
    pieces = load_pieces()
    results = pieces.map(download_and_transform)
    return merge(results.collect())

defs = Definitions(
    assets=[dynamic_graph_asset],
)
