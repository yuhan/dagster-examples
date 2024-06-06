# from typing import Generator, Tuple, List

# import polars as pl
# from dagster import (
#     Definitions,
#     DynamicOut,
#     DynamicOutput,
#     graph_asset,
#     op,
#     Out,
#     ConfigurableIOManager,InputContext,OutputContext
# )
# from dagster_polars import PolarsParquetIOManager
# from upath import UPath

# class ExtendedPolarsParquetIOManager(ConfigurableIOManager):
#     base_dir: str

#     def _make_directory(self, path: "UPath"):
#         """Create a directory at the provided path.

#         Override as a no-op if the target backend doesn't use directories.
#         """
#         path.mkdir(parents=True, exist_ok=True)

#     def handle_output(self, context: OutputContext, obj):
#         path = self._get_path(context)
#         self._make_directory(path.parent)
#         print("output path", path)
#         obj.collect().write_parquet(path)

#     def load_input(self, context: InputContext):
#         path = self._get_path(context.upstream_output) # get output's output context
#         print("input path", path)
#         return pl.scan_parquet(path)

#     def _get_path(self, context: OutputContext, idx=None):
#         return UPath(self.base_dir, *context.get_identifier()) # instead of context.asset_key, use context.get_identifier() because it's in op not asset.

# #### dummy functions for repro purposes
# def load_filenames() -> list[Tuple[str, str]]:
#     return [(str(i), f"file_{i}.csv") for i in range(2)]

# def some_function_that_does_that(filename: str) -> pl.LazyFrame:
#     return pl.LazyFrame({"a": [1, 2, 3], "b": [4, 5, 6]})


# @op(out=DynamicOut(str))
# def load_pieces() -> Generator[DynamicOut, None, None]:
#     filenames = load_filenames()
#     for idx, fn in filenames:
#         yield DynamicOutput(fn, mapping_key=idx)

# @op(out=Out(io_manager_key="polars_parquet_io_manager")) # specify io_manager_key here so it handles both output storing and input loading
# def download_and_transform(filename: str) -> pl.LazyFrame:
#     return some_function_that_does_that(filename)

# @op
# def merge(dfs: List[pl.LazyFrame]) -> pl.LazyFrame:
#     print(dfs)
#     foo = pl.concat(dfs)
#     print("foo", foo)
#     return foo

# @graph_asset
# def dynamic_graph_asset():
#     pieces = load_pieces()
#     results = pieces.map(download_and_transform)
#     return merge(results.collect())



# defs = Definitions(
#     assets=[dynamic_graph_asset],
#     resources={
#         "polars_parquet_io_manager": ExtendedPolarsParquetIOManager(base_dir="/tmp/polars_parquet_io_manager")
#     }
# )

from typing import TypeVar, Generic
import pydantic
from pydantic import BaseModel, ValidationError, PrivateAttr
from dagster import ConfigurableResource, InitResourceContext, Definitions, asset, job

# Define the generic Pydantic model
CredentialsT = TypeVar('CredentialsT', bound=BaseModel)

class BasicCredentials(BaseModel):
    username: str
    password: str

class GenericCredentials(BaseModel, Generic[CredentialsT]):
    credentials: CredentialsT

# Create the ConfigurableResource class using the generic model
class CredentialsResource(ConfigurableResource, Generic[CredentialsT]):
    _credentials: GenericCredentials[CredentialsT] = PrivateAttr()

    @property
    def credentials(self) -> CredentialsT:
        return self._credentials.credentials

    def setup_for_execution(self, context: InitResourceContext) -> None:
        dummy_secret = {"username": "Me", "password": "MyPassword"}
        try:
            self._credentials = GenericCredentials[CredentialsT](credentials=CredentialsT(**dummy_secret))
        except ValidationError as e:
            raise ValueError(f"Invalid credentials: {e}")

# Example usage
credentials = CredentialsResource[BasicCredentials]()

# Using the resource in Dagster
@asset
def my_asset(credentials: CredentialsResource[BasicCredentials]) -> None:
    creds = credentials.credentials
    print(creds.username, creds.password)

defs = Definitions(
    assets=[my_asset],
    resources={"credentials": CredentialsResource[BasicCredentials](username="Me", password="MyPassword")}
)

@job
def my_job():
    my_asset()

if __name__ == "__main__":
    result = my_job.execute_in_process()