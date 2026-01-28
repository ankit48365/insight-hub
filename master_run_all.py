
from other.weather_json_create import save_flattened_weather_next_week_imperial as get_weather
from other.weather_json_fix import fix_it as fix_it
from pipeline.pipeline_strava import load_strava as load_strava
from other.refresh_acc_token_gsm import refresh_access_token as refresh_token



def run_all():
    print("Starting weather json ...")
    get_weather()
    fix_it()
    print("starting refresh strava access code ")
    refresh_token()
    print("strava pipeline starting...")
    load_strava()
    print("Pipeline completed.")

if __name__ == "__main__":
    run_all()