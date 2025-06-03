import os
import json
import time
import requests
from datetime import datetime, timedelta
from kafka import KafkaProducer
from dotenv import load_dotenv

# Load environment variables
load_dotenv("config/.env")
NEWSAPI_KEY = os.getenv("NEWSAPI_KEY")
KAFKA_TOPIC = "newsapi_articles"
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")

# Setup Kafka producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# Keywords and parameters
QUERY = "wildfire OR Waldbrand OR landslide OR Erdrutsch"
LANGUAGES = "de,en"
DAYS = 3

# Build time range
from_date = (datetime.utcnow() - timedelta(days=DAYS)).strftime("%Y-%m-%d")
to_date = datetime.utcnow().strftime("%Y-%m-%d")

# NewsAPI endpoint
url = (
    f"https://newsapi.org/v2/everything?q={QUERY}&"
    f"from={from_date}&to={to_date}&"
    f"language={LANGUAGES}&sortBy=publishedAt&apiKey={NEWSAPI_KEY}"
)

print(f"\U0001F50D Fetching articles from NewsAPI... [{from_date} to {to_date}]")
response = requests.get(url)

if response.status_code != 200:
    print(f"❌ NewsAPI fetch failed: {response.status_code} - {response.text}")
    exit()

articles = response.json().get("articles", [])
print(f"✅ Fetched {len(articles)} articles. Publishing to Kafka...")

for article in articles:
    record = {
        "title": article.get("title"),
        "description": article.get("description"),
        "url": article.get("url"),
        "publishedAt": article.get("publishedAt"),
        "source": article.get("source", {}).get("name"),
        "language": LANGUAGES,
        "timestamp": datetime.utcnow().isoformat(),
        "source_type": "newsapi"
    }
    producer.send(KAFKA_TOPIC, record)
    print(f"🚀 Sent: {record['title'][:80]}...")
    time.sleep(0.2)  # prevent throttling

print("✅ All articles sent to Kafka.")
producer.flush()