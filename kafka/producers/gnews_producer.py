# kafka/producers/gnews_producer.py

import os
import json
import requests
from kafka import KafkaProducer
from datetime import datetime
from dotenv import load_dotenv

# Load environment
load_dotenv("config/.env")
GNEWS_API_KEY = os.getenv("GNEWS_API_KEY")
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_GNEWS_TOPIC", "gnews_articles")

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

print("🚀 Starting GNews producer...")

DISASTER_KEYWORDS = [
    "wildfire", "landslide", "Waldbrand", "Erdrutsch",
    "Flächenbrand", "Bergsturz", "Valais", "Blatten"
]

COUNTRIES = ["Germany", "Switzerland", "Deutschland", "Schweiz"]
LANGUAGES = ["en", "de"]

for keyword in DISASTER_KEYWORDS:
    for country in COUNTRIES:
        for lang in LANGUAGES:
            query = f"{keyword} {country}"
            url = f"https://gnews.io/api/v4/search?q={query}&lang={lang}&token={GNEWS_API_KEY}"

            try:
                response = requests.get(url, timeout=10)
                if response.status_code == 200:
                    articles = response.json().get("articles", [])
                    for article in articles:
                        article_record = {
                            "source_type": "gnews",
                            "source": article.get("source", {}).get("name", ""),
                            "title": article.get("title", ""),
                            "description": article.get("description", ""),
                            "url": article.get("url", ""),
                            "publishedAt": article.get("publishedAt", datetime.utcnow().isoformat()),
                            "timestamp": datetime.utcnow().isoformat(),
                            "query": query,
                            "language": lang
                        }
                        producer.send(KAFKA_TOPIC, value=article_record)
                        print(f"📤 Sent article from GNews: {article_record['title'][:80]}")
                else:
                    print(f"❌ Error {response.status_code}: {response.text}")
            except Exception as e:
                print(f"❌ Failed to fetch GNews articles: {e}")

print("✅ Finished sending GNews articles.")
producer.flush()
