from typing import Any
import dlt
from dlt.sources.rest_api import RESTAPIConfig, rest_api_resources, check_connection


@dlt.source(name="strava")
def strava_source() -> Any:
    """DLT source for Strava REST API."""
    
    sec = dlt.secrets.get("sources.strava")
    access_token = sec["access_token"]

    config: RESTAPIConfig = {
        "client": {
            "base_url": "https://www.strava.com/api/v3/",
            "auth": (
                {
                    "type": "bearer",
                    "token": access_token,
                } if access_token else None
            ),
        },
        "resource_defaults": {
            "primary_key": "id",
            "write_disposition": {"disposition": "merge", "strategy": "upsert"},
            "endpoint": {
                "params": {
                    "per_page": 200
                },
            },
        },
        "resources": [
            {
                "name": "activities",
                "endpoint": {
                    "path": "athlete/activities",
                    "params": {
                        "per_page": 200
                    },
                },
            }
        ],
    }

    yield from rest_api_resources(config)