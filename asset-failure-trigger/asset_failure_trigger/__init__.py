from typing import Sequence

from dagster import (
    DagsterEventType,
    Definitions,
    EnvVar,
    EventLogRecord,
    RunFailureSensorContext,
    SkipReason,
    load_assets_from_modules,
    run_failure_sensor,
)
from dagster_pagerduty import PagerDutyService

from . import assets

all_assets = load_assets_from_modules([assets])


@run_failure_sensor
def on_asset_failure(context: RunFailureSensorContext, pagerduty: PagerDutyService):
    """
    This sensor monitor all failed runs in the current code location.
    For each failed run, it checks if any asset materialization missing and determine the failed assets. Then it sends a pagerduty alert for each failed asset.
    """
    failed_run_id = context.dagster_run.run_id
    records: Sequence[EventLogRecord] = context.instance.get_records_for_run(
        failed_run_id,
        of_type={
            DagsterEventType.ASSET_MATERIALIZATION,
            DagsterEventType.ASSET_MATERIALIZATION_PLANNED,
        },
    ).records

    # Can change the values to include more info like failure message
    planned_asset_keys = {
        record.event_log_entry.dagster_event.asset_key
        for record in records
        if record.event_log_entry.dagster_event.event_type_value
        == DagsterEventType.ASSET_MATERIALIZATION_PLANNED
    }

    successful_asset_keys = {
        record.event_log_entry.dagster_event.asset_key
        for record in records
        if record.event_log_entry.dagster_event.event_type_value
        == DagsterEventType.ASSET_MATERIALIZATION
    }

    # Skip if all assets were successfully materialized. the run failure sensor is triggered because of non-asset related
    if len(planned_asset_keys) == len(successful_asset_keys):
        return SkipReason("All assets were successfully materialized")

    # Find asset materialization failure
    failed_asset_keys = planned_asset_keys.difference(successful_asset_keys)

    # Send pagerduty alert for each failed asset
    for asset_key in failed_asset_keys:
        pagerduty.EventV2_create(
            summary=f"Asset {asset_key} failed",
            source="dagster",
            severity="error",
            event_action="trigger",
        )
        context.log.info(f"Sent pagerduty alert for asset {asset_key}")


defs = Definitions(
    assets=all_assets,
    sensors=[on_asset_failure],
    resources={"pagerduty": PagerDutyService(routing_key=EnvVar("PD_ROUTING_KEY"))},
)
