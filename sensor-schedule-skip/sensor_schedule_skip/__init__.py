from dagster import (
    AssetExecutionContext,
    AssetKey,
    Definitions,
    RunFailureSensorContext,
    RunRequest,
    asset,
    define_asset_job,
    run_failure_sensor,
)


@asset
def my_upstream_asset() -> int:
   return 1
   
@asset
def my_downstream_asset(context: AssetExecutionContext, my_upstream_asset: int) -> None:
  # if tags are passed through, pass this asset
  if context.run_tags.get("ecs/cpu") == "256":
    return
  raise Exception("failed!")


my_asset_job = define_asset_job(
    "my_asset_job",
    selection=[my_upstream_asset, my_downstream_asset],
)

  
# @run_failure_sensor(monitored_jobs=[my_asset_job], request_job=my_asset_job)
# def multi_asset_sensor(context: RunFailureSensorContext):
#     # get all events for the failed run
#     all_records_for_failed_run = context.instance.get_records_for_run(
#         run_id=context.dagster_run.run_id,
#     ).records

#     # find all the failed assets
#     all_failed_assets = []
#     for record in all_records_for_failed_run:
#         dagster_event = record.event_log_entry.dagster_event
#         if dagster_event and dagster_event.is_step_failure and dagster_event.step_key:
#             print(dagster_event.step_key)
#             all_failed_assets.append(AssetKey(dagster_event.step_key))
    
#     # this would launch a new run with the updated tags and only run the selected assets
#     return RunRequest(
#        run_key=context.dagster_run.run_id, # this is the cursor so we don't get triggered by the same run again
#        tags={
#           "ecs/cpu": "256",
#           "ecs/memory": "512",
#           # thew two tags link the new run to the failed run so we get the re-execution lineage
#           "dagster/root_run_id": context.dagster_run.root_run_id or context.dagster_run.run_id, 
#           "dagster/parent_run_id": context.dagster_run.run_id,
#         }, # tags to bump the task resources
#         asset_selection=all_failed_assets
#     )


@run_failure_sensor
def multi_asset_sensor(context: RunFailureSensorContext):
   print("hello!!!!!!!!!!!")



defs = Definitions(
    assets=[my_upstream_asset, my_downstream_asset],
    jobs=[my_asset_job],
    sensors=[multi_asset_sensor],
)
