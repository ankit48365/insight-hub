import os
import dlt
import feedparser
import urllib.parse
from google.cloud import secretmanager
from dlt.sources.helpers import requests

def get_secret_topics():
    """Fetches the comma-separated list from Secret Manager."""
    client = secretmanager.SecretManagerServiceClient()
    # project_id = os.getenv("GCP_PROJECT_ID")
    # Change 'NEWS_TOPICS_SECRET' to your actual secret name
    name = f"projects/mystrava-464501/secrets/RSS_FEED_TOPICS/versions/latest"
    
    try:
        response = client.access_secret_version(request={"name": name})
        payload = response.payload.data.decode("UTF-8")
        # Clean up each topic to remove extra spaces or newlines
        # Inside your get_secret_topics function
        print(f"DEBUG: Payload from Secret Manager: '{payload}'")
        topics = [t.strip() for t in payload.split(",") if t.strip()]
        print(f"DEBUG: Final Topic List: {topics}")

        return topics
    except Exception as e:
        print(f"ERROR: Could not access Secret Manager: {e}")
        return []

@dlt.source
def google_news_source():
    # 1. Fetch values: Topics from Secret, Timespan from Env (or default)
    topics = get_secret_topics()
    timespan = os.getenv("NEWS_TIMESPAN", "1d")
    
    # We define the resource logic inside a generator function
    def fetch_news(topic):
        encoded_topic = urllib.parse.quote(topic)
        rss_url = f"https://news.google.com/rss/search?q=intitle:\"{encoded_topic}\"+when:{timespan}&hl=en-US&gl=US&ceid=US:en"
        # if you remove "intitle:" from url above, then it will do sentiment type search, if context match, it will extract that result
        response = requests.get(rss_url)
        feed = feedparser.parse(response.text)
        
        for entry in feed.entries:
            yield {
                "topic": topic,
                "title": entry.title,
                "link": entry.link,
                "guid": entry.id,
                "pubDate": entry.published,
                "source": {
                    "url": entry.source.get('url', '') if hasattr(entry, 'source') else '',
                    "name": entry.source.get('title', '') if hasattr(entry, 'source') else ''
                }
            }

    # 2. Yield a resource for every topic in your secret list
    for topic in topics:
        table_name = topic.lower().replace(" ", "_")
        yield dlt.resource(
            fetch_news(topic), 
            name=table_name, 
            write_disposition="append"
        )

