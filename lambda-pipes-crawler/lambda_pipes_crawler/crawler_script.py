import base64
import json
import os
from io import BytesIO

import matplotlib.pyplot as plt
import pandas as pd
import requests
from dagster import AssetExecutionContext, MaterializeResult, MetadataValue, asset
from dagster_pipes import PipesContext, open_dagster_pipes

"""
1. Fetch client-specific configuration (e.g. filters, API parameters)
2. Fetch object IDs (so the fanout does point queries)
3. Fanout - For every object ID, generate the object, store in S3
4. Mark all as complete
5. Post-processing

Requirements
* Dagster should kickoff the fanout step
* Dagster must observe the fanout status
* Dagster must know when fanout step has completed
* Dagster should be able to determine the number of failed (i.e. DLQ'd) API calls
* Redriving failed (DLQ'd) API calls should take minimal effort
* API crawler must be able to retry individual requests
* A single failed client's materialization must not impact the materialization of other clients
* A single failed API call must be retried up to 3 times before marking it as failed
* A single failed API call must not block other API calls from trying to complete
* A single failed API call must result is failed asset materialization

Relative Scale
* Daily API calls > 1 billion
* Clients = 40-ish
* API crawls may take hours to complete
* 2 current API crawler workflows
"""

def main():
    # get the Dagster Pipes context
    context = PipesContext.get()

    # base layer 0
    newstories_url = "https://hacker-news.firebaseio.com/v0/topstories.json"
    ls_of_item_ids = requests.get(newstories_url).json()[:100]

    results: list[str] = []

    for item_id in ls_of_item_ids:
        item = requests.get(f"https://hacker-news.firebaseio.com/v0/item/{item_id}.json").json()
        results.append(item)

        if len(results) % 20 == 0:
            context.report_custom_message({"count": f"Got {len(results)} items so far."})

    context.report_asset_materialization(metadata={"total_orders": len(orders)})


if __name__ == "__main__":
    # connect to Dagster Pipes
    with open_dagster_pipes():
        main()
