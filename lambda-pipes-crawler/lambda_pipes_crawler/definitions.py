import shutil

import pandas as pd
from dagster import (
    AssetExecutionContext,
    AutoMaterializePolicy,
    Definitions,
    Output,
    PipesSubprocessClient,
    asset,
    file_relative_path,
)


@asset
def asset1():
    print("hello 1")


@asset(auto_materialize_policy=AutoMaterializePolicy.eager(), deps=[asset1])
def asset2():
    print("hello 2")


defs = Definitions(
    assets=[asset1, asset2],
)


# @asset
# def subprocess_asset(
#     context: AssetExecutionContext,
#     pipes_subprocess_client: PipesSubprocessClient,
# ) -> Output[pd.DataFrame]:
#     cmd = [shutil.which("python"), file_relative_path(__file__, "crawler_script.py")]
#     result = pipes_subprocess_client.run(
#         command=cmd,
#         context=context,
#     )

#     # a small summary table gets reported as a custom message
#     messages = result.get_custom_messages()
#     if len(messages) != 1:
#         raise Exception("summary not reported")

#     summary_df = pd.DataFrame(messages[0])

#     # grab any reported metadata off of the materialize result
#     metadata = result.get_materialize_result().metadata

#     # return the summary table to be loaded by Dagster for downstream assets
#     return Output(
#         value=summary_df,
#         metadata=metadata,
#     )


# defs = Definitions(
#     assets=[subprocess_asset],
#     resources={"pipes_subprocess_client": PipesSubprocessClient()},
# )
