from dagster import (
   AssetExecutionContext, 
    RunConfig, 
    Config, 
    Definitions, 
    MaterializeResult, 
    define_asset_job, 
    build_schedule_from_partitioned_job, 
    DailyPartitionsDefinition, 
    asset, 
    ScheduleEvaluationContext, 
    schedule, 
    SkipReason, 
    RunRequest,
)
from dagster_aws.s3 import S3Resource
from typing import Optional


# Define your asset with daily partitions
# end_offset=1 means that the last partition will be today (rather than day-1)
daily_partitions_def = DailyPartitionsDefinition(start_date="2024-01-01", end_offset=1)

@asset(partitions_def=daily_partitions_def)
def my_upstream_asset() -> None:
    # Logic to load data for the partition
    ...

class MyAssetConfig(Config):
    number_of_files_detected: Optional[int]

@asset(partitions_def=daily_partitions_def, deps=[my_upstream_asset])
def my_downstream_asset(context: AssetExecutionContext, config: MyAssetConfig) -> MaterializeResult:
    # Logic to load data for the partition
    return MaterializeResult(
        metadata={
            "num_files": config.number_of_files_detected,
        }
    )


my_asset_job = define_asset_job(
    "my_asset_job",
    selection=[my_upstream_asset, my_downstream_asset],
    partitions_def=daily_partitions_def,
)

# Define a schedule for daily updates
daily_updates_schedule = build_schedule_from_partitioned_job(
    job=my_asset_job,
)

# Define a schedule for more frequent updates. skip if no new data 
@schedule(job=my_asset_job, cron_schedule="0 6-18 * * 1-5", execution_timezone="US/Pacific")
def more_frequent_schedule(context: ScheduleEvaluationContext, s3: S3Resource):
  # check if s3 files are complete, for example:
  # s3.get_client().list_objects(Bucket="my-bucket", Prefix="my-prefix")
  # and maybe pass down the new number of files detected
  number_of_files_detected = 100
  IS_S3_COMPLETE = True
  if not IS_S3_COMPLETE:
    return SkipReason("S3 files not complete")
  
  return RunRequest(
    run_key="some_run_key",
    run_config=RunConfig({'my_downstream_asset': MyAssetConfig(number_of_files_detected=number_of_files_detected)}),
    partition_key=daily_partitions_def.get_partition_key_for_timestamp(context.scheduled_execution_time.timestamp())
  )

defs = Definitions(
    assets=[my_upstream_asset, my_downstream_asset],
    jobs=[my_asset_job],
    schedules=[daily_updates_schedule, more_frequent_schedule],
    resources={"s3": S3Resource},
)
