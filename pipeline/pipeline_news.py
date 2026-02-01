import dlt
from api_source.news_feed_rss import google_news_source


def load_news() -> None:
    """
    Initializes a dlt pipeline to load Strava data into BigQuery.

    This function sets up the pipeline, creates the Strava source,
    and then runs the pipeline to load the data. It will print
    the outcome of the load operation.
    """
    pipeline = dlt.pipeline(
        pipeline_name="news_pipeline",
        destination="bigquery",
        dataset_name="landing",
    )

    # Create the source. Credentials will be loaded automatically
    # by dlt from your configured secrets provider (e.g., Google Secret Manager).
    data_source = google_news_source()
    print("Starting news data load...")
    load_info = pipeline.run(data_source, table_name="src_gcloud_job_news")

    # Print the results
    print(load_info)

if __name__ == "__main__":
    load_news()