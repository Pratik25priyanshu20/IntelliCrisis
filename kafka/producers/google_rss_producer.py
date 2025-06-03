import os
import json
import time
import logging
import feedparser
from kafka import KafkaProducer
from datetime import datetime
from urllib.parse import quote_plus

# Logger setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Kafka setup
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_GOOGLE_RSS_TOPIC", "google_rss_articles")
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# Keywords for disaster events
KEYWORDS = [
    "wildfire Germany", "wildfire Switzerland", "Waldbrand Deutschland", "Waldbrand Schweiz",
    "landslide Germany", "landslide Switzerland", "Erdrutsch Deutschland", "Erdrutsch Schweiz"
]

logger.info("🚀 Starting Google News RSS ingestion...")
inserted_total = 0

for keyword in KEYWORDS:
    logger.info(f"🔍 Searching: {keyword}")
    encoded_keyword = quote_plus(keyword)
    feed_url = f"https://news.google.com/rss/search?q={encoded_keyword}&hl=de&gl=DE&ceid=DE:de"

    try:
        feed = feedparser.parse(feed_url)
        count = 0

        for entry in feed.entries:
            doc = {
                "title": entry.title,
                "description": entry.get("summary", ""),
                "url": entry.link,
                "publishedAt": entry.get("published", str(datetime.utcnow())),
                "source": "Google News RSS",
                "source_type": "google_rss",
                "query": keyword,
                "timestamp": str(datetime.utcnow())
            }
            producer.send(KAFKA_TOPIC, value=doc)
            count += 1

        inserted_total += count
        logger.info(f"🗞️ Inserted {count} articles for query '{keyword}'")
        time.sleep(1)

    except Exception as e:
        logger.warning(f"❌ Failed to fetch for '{keyword}': {e}")

logger.info(f"✅ Google RSS ingestion complete. Total articles inserted: {inserted_total}")
