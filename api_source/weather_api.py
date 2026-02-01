# api_source/weather_api.py
import dlt
from datetime import date, timedelta
import requests
from typing import Iterator, Any

@dlt.source(name="openmeteo_concord_nh")
def concord_nh_weather_rest():
    @dlt.resource(name="src_gcloud_job_daily_weather")
    def daily_weather() -> Iterator[Any]:
        lat, lon = 43.2081, -71.5376
        start_date = date.today()
        end_date = start_date + timedelta(days=15)

        url = (
            f"https://api.open-meteo.com/v1/forecast"
            f"?latitude={lat}&longitude={lon}"
            f"&daily="
            f"temperature_2m_max,temperature_2m_min,"
            f"apparent_temperature_max,apparent_temperature_min,"
            f"precipitation_sum,rain_sum,snowfall_sum,"
            f"windspeed_10m_max,sunrise,sunset"
            f"&temperature_unit=fahrenheit"
            f"&windspeed_unit=mph"
            f"&precipitation_unit=inch"
            f"&start_date={start_date}&end_date={end_date}"
            f"&timezone=America/New_York"
        )

        response = requests.get(url)
        response.raise_for_status()
        daily = response.json().get("daily", {})

        times = daily.get("time", [])

        for i, date_str in enumerate(times):
            yield {
                "date": date_str,
                "city": "Concord",
                "state": "New Hampshire",
                "country": "United States",
                "latitude": lat,
                "longitude": lon,

                # Temperature
                "temp_max_f": daily.get("temperature_2m_max", [None])[i],
                "temp_min_f": daily.get("temperature_2m_min", [None])[i],
                "apparent_temp_max_f": daily.get("apparent_temperature_max", [None])[i],
                "apparent_temp_min_f": daily.get("apparent_temperature_min", [None])[i],

                # Precipitation
                "precipitation_in": daily.get("precipitation_sum", [None])[i],
                "rain_sum_in": daily.get("rain_sum", [None])[i],
                "snowfall_sum_in": daily.get("snowfall_sum", [None])[i],

                # Wind
                "windspeed_max_mph": daily.get("windspeed_10m_max", [None])[i],

                # Sunlight
                "sunrise": daily.get("sunrise", [None])[i],
                "sunset": daily.get("sunset", [None])[i],
            }

    return daily_weather