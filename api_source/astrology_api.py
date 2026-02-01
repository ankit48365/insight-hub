import dlt
import requests
from datetime import datetime, timedelta, date
from typing import Iterator, Any

API_URL = "https://json.freeastrologyapi.com/rahu-kalam"
API_KEY = "VT0mrAZWf64oIqoTgUfZg6AmH91Cfasv87YYOxbR"

LOCATION = {
    "latitude": 43.2081,
    "longitude": -71.5376,
    "timezone": -5
}

HEADERS = {
    "Content-Type": "application/json",
    "x-api-key": API_KEY
}

@dlt.source(name="rahu_kalam_concord_nh")
# def rahu_kalam_rest(start_date: str, end_date: str):
def rahu_kalam_rest():


    @dlt.resource(name="src_gcloud_job_rahu")
    def daily_rahu_kalam() -> Iterator[Any]:
        start_date = date.today()
        end_date = start_date + timedelta(days=5)
        current = start_date

        while current <= end_date:
            payload = {
                "year": current.year,
                "month": current.month,
                "date": current.day,
                "hours": 12,
                "minutes": 1,
                "seconds": 0,
                "latitude": LOCATION["latitude"],
                "longitude": LOCATION["longitude"],
                "timezone": LOCATION["timezone"]
            }

            try:
                response = requests.post(API_URL, json=payload, headers=HEADERS)
                response.raise_for_status()
                data = response.json()
            except Exception:
                data = None

            if data:
                # Flatten the API response for analytics
                yield {
                    "date": current.strftime("%Y-%m-%d"),
                    "city": "Concord",
                    "state": "New Hampshire",
                    "country": "United States",
                    "latitude": LOCATION["latitude"],
                    "longitude": LOCATION["longitude"],
                    "timezone": LOCATION["timezone"],

                    # Raw API fields (flattened)
                    "starts_at": data.get("starts_at"),
                    "ends_at": data.get("ends_at"),
                    "duration_minutes": data.get("duration"),
                    "weekday": data.get("weekday"),
                    "sunrise": data.get("sunrise"),
                    "sunset": data.get("sunset"),
                    "raw_json": data  # optional: keep full payload
                }

            current += timedelta(days=1)

    return daily_rahu_kalam