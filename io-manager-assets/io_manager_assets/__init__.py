# from .definitions import defs
from dagster import AssetExecutionContext, Definitions, asset, define_asset_job


@asset
def hello_logs(context: AssetExecutionContext) -> None:
    context.log.info("Hello, world!")


asset_job = define_asset_job(name="asset_job")

defs = Definitions(assets=[hello_logs], jobs=[asset_job])