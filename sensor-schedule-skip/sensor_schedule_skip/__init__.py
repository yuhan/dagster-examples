from dagster import Definitions, RunRequest, load_assets_from_modules, schedule, ScheduleEvaluationContext, SkipReason, define_asset_job

from . import assets

all_assets = load_assets_from_modules([assets])

# fake complete marker of external s3 files
IS_S3_COMPLETE = True

all_asset_job = define_asset_job("all_asset_job")

@schedule(job=all_asset_job, cron_schedule="*/10 6-8 * * *", execution_timezone="US/Pacific")
def my_schedule(context: ScheduleEvaluationContext):
  # check if s3 files are complete
  if not IS_S3_COMPLETE:
    return SkipReason("S3 files not complete")
  
  return RunRequest(run_key="some_run_key")
  

defs = Definitions(
    assets=all_assets,
    schedules=[my_schedule],
)