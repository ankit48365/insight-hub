"""
dlt_strava_bigquery.py

A custom DLT source for Strava data using the declarative REST API source,
loaded into BigQuery.
"""

from typing import Any

import dlt
from dlt.sources.rest_api import (
    RESTAPIConfig,
    rest_api_source,
    check_connection,
)

@dlt.source(name="strava")
def strava_source() -> Any:
    """
    Defines a Strava source using dlt's REST API declarative configuration.
    Credentials are resolved automatically via:
      - dlt.secrets ("sources.strava.access_token")
      - or environment: SOURCES__STRAVA__ACCESS_TOKEN
    """
    config: RESTAPIConfig = {
        "client": {
            "base_url": "https://www.strava.com/api/v3/",
            # auth will be automatically filled from secrets / env if not set here
            # "auth": {"type": "bearer", "token": dlt.secrets.value}  # ← this also works
        },
        "resource_defaults": {
            "primary_key": "id",
            "write_disposition": "merge",           # or {"disposition": "merge", "strategy": "upsert"}
            "endpoint": {
                "params": {
                    "per_page": 200,                # Strava max is 200
                },
            },
        },
        "resources": [
            {
                "name": "activities",
                "endpoint": {
                    "path": "athlete/activities",
                    # You can add more params if needed, e.g. before/after dates
                    # "params": {"before": ..., "after": ..., "page": 1}
                },
            },
            # You can easily add more endpoints later, e.g.:
            # {
            #     "name": "athlete",
            #     "endpoint": {"path": "athlete"},
            # },
            # {
            #     "name": "activity_streams",
            #     "endpoint": {
            #         "path": "activities/{id}/streams",
            #         "params": {"keys": "time,latlng,altitude,heartrate"},
            #     },
            #     "parent": "activities",   # ← enables incremental child loading
            # },
        ],
    }

    # This creates the actual source object
    return rest_api_source(config)


def load_strava() -> None:
    """Initializes the DLT pipeline and loads Strava data into BigQuery."""
    pipeline = dlt.pipeline(
        pipeline_name="strava_pipeline",
        destination="bigquery",
        dataset_name="landing",
        # Optional but recommended:
        # full_refresh=True,                # ← for testing
        # pipeline_type="direct",           # faster for small loads
    )

    # Create the source instance (dlt auto-resolves credentials)
    source = strava_source()

    # Optional: test connection (very useful during dev)
    can_connect, error_msg = check_connection(source, "activities")

    if not can_connect:
        raise RuntimeError(f"Cannot connect to Strava API → {error_msg}")

    print("Connection check passed. Starting load...")

    # Run the pipeline
    load_info = pipeline.run(source)

    # Print nice summary
    print(load_info)
    print(f"Loaded {len(load_info.jobs['completed_jobs'])} resources successfully.")


if __name__ == "__main__":
    try:
        load_strava()
    except Exception as exc:
        print(f"Pipeline failed: {exc}")
        raise