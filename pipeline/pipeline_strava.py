import dlt
from api_source.strava_api import strava_source 

def load_strava() -> None:
    """
    Initializes a dlt pipeline to load Strava data into BigQuery.

    This function sets up the pipeline, creates the Strava source,
    and then runs the pipeline to load the data. It will print
    the outcome of the load operation.
    """
    pipeline = dlt.pipeline(
        pipeline_name="strava_pipeline",
        destination="bigquery",
        dataset_name="landing",
    )

    # Create the source. Credentials will be loaded automatically
    # by dlt from your configured secrets provider (e.g., Google Secret Manager).
    data_source = strava_source()

    print("Starting Strava data load...")
    load_info = pipeline.run(data_source)

    # Print the results
    print(load_info)

if __name__ == "__main__":
    load_strava()