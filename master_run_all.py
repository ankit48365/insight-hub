
# from other.weather_json_create import save_flattened_weather_next_week_imperial as get_weather
# from other.weather_json_fix import fix_it as fix_it
from pipeline.pipeline_strava import load_strava as load_strava
from other.refresh_acc_token_gsm import refresh_access_token as refresh_token

from pipeline.pipeline_weather import load_weather
from pipeline.pipeline_astro import load_astro

def run_all():

    print("starting refresh strava access code ")
    refresh_token()
    print("strava pipeline starting...")
    load_strava()
    print("starting weather api extract ")
    load_weather()
    print("starting astro api extract...")
    load_astro()
    print("Pipeline completed.")

if __name__ == "__main__":
    run_all()