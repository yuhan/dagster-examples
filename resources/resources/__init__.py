from dagster import (
    AssetKey,
    ConfigurableResource,
    Definitions,
    EnvVar,
    RunRequest,
    asset,
    define_asset_job,
    sensor,
)

# @asset
# def asset1():
#     return [1, 2, 3]


# @asset
# def asset2():
#     return [4, 5]


# all_asset_job = define_asset_job(name="all_asset_job", selection="*")

# @sensor(job=all_asset_job)
# def my_sensor():
#     # certain conditions are met
#     if ...:
#         # this will only kick off asset1
#         yield RunRequest(asset_selection=[AssetKey("asset1")])

class DagsterResource(ConfigurableResource):
    endpoint: str = EnvVar("ENDPOINT")

@asset
def my_asset(my_resource: DagsterResource):
    print(my_resource.endpoint)

defs = Definitions(assets=[my_asset], resources={"my_resource": DagsterResource()})