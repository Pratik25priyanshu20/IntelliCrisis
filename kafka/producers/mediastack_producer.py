import os
import json
import time
import requests
from dotenv import load_dotenv
from kafka import KafkaProducer
from datetime import datetime

# Load environment variables
load_dotenv("config/.env")
MEDIASTACK_KEY = os.getenv("MEDIASTACK_KEY")
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
TOPIC = "mediastack_articles"

# Kafka producer setup
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

KEYWORDS = ["Waldbrand", "Vegetationsbrand", "Flächenbrand", "Feuer", "Erdrutsch", "landslide"]
COUNTRIES = ["de", "ch"]  # Germany, Switzerland
LANGUAGES = ["en", "de"]

print("🌐 Starting Mediastack Kafka producer...")

for keyword in KEYWORDS:
    for country in COUNTRIES:
        for lang in LANGUAGES:
            url = (
                f"http://api.mediastack.com/v1/news?"
                f"access_key={MEDIASTACK_KEY}&"
                f"keywords={keyword}&"
                f"languages={lang}&"
                f"countries={country}&"
                f"limit=50"
            )
            print(f"🔍 Fetching: {keyword} [{country.upper()} - {lang}]")
            response = requests.get(url)
            if response.status_code != 200:
                print(f"❌ Error {response.status_code}: {response.text}")
                continue

            articles = response.json().get("data", [])
            print(f"📰 Articles found: {len(articles)}")

            for article in articles:
                record = {
                    "title": article.get("title"),
                    "description": article.get("description"),
                    "url": article.get("url"),
                    "publishedAt": article.get("published_at"),
                    "source": article.get("source"),
                    "country": country,
                    "language": lang,
                    "query": keyword,
                    "source_type": "mediastack",
                    "timestamp": datetime.utcnow().isoformat()
                }
                producer.send(TOPIC, value=record)
                print(f"🚀 Sent to Kafka: {record['title'][:60]}...")
            time.sleep(1)  # prevent rate limiting

print("✅ Mediastack producer finished.")
producer.close()