import os

import pandas as pd
from dagster import (
    ConfigurableIOManager,
    DagsterEventType,
    Definitions,
    InputContext,
    Output,
    OutputContext,
    asset,
)


@asset(io_manager_key="df_to_parquet_io_manager")
def lambda_asset():
    json_val = {'col_1': [3, 2, 1, 0], 'col_2': ['a', 'b', 'c', 'd']}
    df = pd.DataFrame.from_dict(json_val)
    return Output(
        df,
        metadata={"foo": "bar"}
    )
# @asset(io_manager_key="df_to_parquet_io_manager")
# def process(lambda_asset):
#     assert type(lambda_asset) == pd.DataFrame
#     return Output(
#         df,
#         metadata={"filepath": "blah.json"}
#     )

####################
# IO Managers
# https://docs.dagster.io/concepts/io-management/io-managers#custom-filesystem-based-io-manager
####################

class PandasParquetIOManager(ConfigurableIOManager):
    def handle_output(self, context: OutputContext, obj):
        events = context.step_context.instance.get_records_for_run(run_id=context.run_id, of_type=DagsterEventType.STEP_OUTPUT, limit=1)
        event_log_entry = events.records[0].event_log_entry
        assert event_log_entry.step_key == context.step_key # making sure it's getting the same step
        metadata = event_log_entry.dagster_event.event_specific_data.metadata
        path_value = metadata["foo"].value
        print(path_value)


        obj.to_parquet(path_value)
        # context.add_output_metadata({"path": path_value})

    def load_input(self, context: InputContext) -> pd.DataFrame:
        asset_key = context.asset_key
        event_log_entry = context.step_context.instance.get_latest_materialization_event(asset_key)
        metadata = event_log_entry.dagster_event.event_specific_data.materialization.metadata
        path_value = metadata["path"].value
        assert path_value == "file/to/path"

        # file_path = self._get_path(context)
        # return load_pandas_dataframe(name=file_path)
        return pd.read_parquet(path_value)

defs = Definitions(
    assets=[lambda_asset],
    resources={
        "df_to_parquet_io_manager": PandasParquetIOManager(),
    }
)