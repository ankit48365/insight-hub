
from weather_json_create import save_flattened_weather_next_week_imperial as get_weather
from weather_json_fix import fix_it as fix_it
from dlt_strava_bquery_2 import load_strava as load_strava



def run_all():
    print("Starting pipeline...")
    get_weather()
    fix_it()
    print("strava pipeline starting...")
    load_strava()
    print("Pipeline completed.")

if __name__ == "__main__":
    run_all()