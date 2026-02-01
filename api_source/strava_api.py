from typing import Any
import dlt
from dlt.sources.rest_api import RESTAPIConfig, rest_api_resources, check_connection
import os
from google.cloud import secretmanager


def get_strava_token():
    # 1. Try Cloud Run env var
    # token = os.getenv("SOURCES__STRAVA__ACCESS_TOKEN")
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/mystrava-464501/secrets/SOURCES__STRAVA__ACCESS_TOKEN/versions/latest"
    response = client.access_secret_version(request={"name": name})
    token = response.payload.data.decode("UTF-8")
    if token:
        return token
   

    # 2. Fallback to local dlt_secrets.toml
    sec = dlt.secrets.get("sources.strava")
    return sec["access_token"]


@dlt.source(name="strava")
def strava_source() -> Any:
    """DLT source for Strava REST API."""
    
    sec = dlt.secrets.get("sources.strava")
    access_token = get_strava_token()


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