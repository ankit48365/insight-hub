import os
import dlt
import feedparser
import urllib.parse
from dlt.sources.helpers import requests

@dlt.source
def google_news_source():
    # 1. Fetch values from Google Cloud Environment Variables
    topic = os.getenv("NEWS_TOPIC", "Bareilly")
    timespan = os.getenv("NEWS_TIMESPAN", "1h")
    
    @dlt.resource(write_disposition="append")
    def news_articles():
        encoded_topic = urllib.parse.quote(topic)
        # Using the variables in the URL
        rss_url = f"https://news.google.com/rss/search?q=intitle:\"{encoded_topic}\"+when:{timespan}&hl=en-US&gl=US&ceid=US:en"
        
        response = requests.get(rss_url)
        feed = feedparser.parse(response.text)
        
        for entry in feed.entries:
            yield {
                "title": entry.title,
                "link": entry.link,
                "guid": entry.id,
                "pubDate": entry.published,
                "source": {
                    "url": entry.source.get('url', '') if hasattr(entry, 'source') else '',
                    "name": entry.source.get('title', '') if hasattr(entry, 'source') else ''
                }
            }
    return news_articles