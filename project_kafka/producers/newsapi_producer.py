'''
import os
import json
import time
import requests
from datetime import datetime, timedelta
from project_kafka import KafkaProducer
from dotenv import load_dotenv

# Load environment variables
load_dotenv("config/.env")

NEWSAPI_KEY = os.getenv("NEWSAPI_KEY")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC_NEWSAPI", "newsapi_articles")
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")

if not NEWSAPI_KEY:
    print("❌ Missing NEWSAPI_KEY. Please check your .env file.")
    exit(1)

# Setup Kafka producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# Keywords and parameters
QUERY = "wildfire OR Waldbrand OR landslide OR Erdrutsch"
LANGUAGES = ["de", "en"]  # Run two separate requests
DAYS = 3

# Time window
from_date = (datetime.utcnow() - timedelta(days=DAYS)).strftime("%Y-%m-%d")
to_date = datetime.utcnow().strftime("%Y-%m-%d")

print(f"🔍 Fetching NewsAPI articles from {from_date} to {to_date}...")

total_articles = 0

for lang in LANGUAGES:
    url = (
        f"https://newsapi.org/v2/everything?q={QUERY}&"
        f"from={from_date}&to={to_date}&"
        f"language={lang}&sortBy=publishedAt&apiKey={NEWSAPI_KEY}"
    )
    
    response = requests.get(url)

    if response.status_code != 200:
        print(f"❌ NewsAPI fetch failed for language '{lang}': {response.status_code} - {response.text}")
        continue

    articles = response.json().get("articles", [])
    print(f"✅ Fetched {len(articles)} articles for language '{lang}'.")

    for article in articles:
        record = {
            "title": article.get("title"),
            "description": article.get("description"),
            "url": article.get("url"),
            "publishedAt": article.get("publishedAt"),
            "source": article.get("source", {}).get("name"),
            "language": lang,
            "query": QUERY,
            "timestamp": datetime.utcnow().isoformat(),
            "source_type": "newsapi"
        }
        producer.send(KAFKA_TOPIC, value=record)
        print(f"🚀 Sent: {record['title'][:80]}...")
        time.sleep(0.2)  # Prevent throttling

    total_articles += len(articles)

print(f"✅ All {total_articles} articles sent to Kafka.")
producer.flush()


'''

import os
import json
import time
import requests
from datetime import datetime, timedelta
from kafka import KafkaProducer  # ✅ Changed this line
from dotenv import load_dotenv

# Load environment variables
load_dotenv("/opt/airflow/config/.env")  # ✅ Fixed path for container

NEWSAPI_KEY = os.getenv("NEWSAPI_KEY")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC_NEWSAPI", "newsapi_articles")
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:29092")  # ✅ Use internal Docker network

if not NEWSAPI_KEY:
    print("❌ Missing NEWSAPI_KEY. Please check your .env file.")
    exit(1)

# Setup Kafka producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# Keywords and parameters
QUERY = "wildfire OR Waldbrand OR landslide OR Erdrutsch"
LANGUAGES = ["de", "en"]  # Run two separate requests
DAYS = 3

# Time window
from_date = (datetime.utcnow() - timedelta(days=DAYS)).strftime("%Y-%m-%d")
to_date = datetime.utcnow().strftime("%Y-%m-%d")

print(f"🔍 Fetching NewsAPI articles from {from_date} to {to_date}...")

total_articles = 0

for lang in LANGUAGES:
    url = (
        f"https://newsapi.org/v2/everything?q={QUERY}&"
        f"from={from_date}&to={to_date}&"
        f"language={lang}&sortBy=publishedAt&apiKey={NEWSAPI_KEY}"
    )
    
    response = requests.get(url)

    if response.status_code != 200:
        print(f"❌ NewsAPI fetch failed for language '{lang}': {response.status_code} - {response.text}")
        continue

    articles = response.json().get("articles", [])
    print(f"✅ Fetched {len(articles)} articles for language '{lang}'.")

    for article in articles:
        record = {
            "title": article.get("title"),
            "description": article.get("description"),
            "url": article.get("url"),
            "publishedAt": article.get("publishedAt"),
            "source": article.get("source", {}).get("name"),
            "language": lang,
            "query": QUERY,
            "timestamp": datetime.utcnow().isoformat(),
            "source_type": "newsapi"
        }
        producer.send(KAFKA_TOPIC, value=record)
        print(f"🚀 Sent: {record['title'][:80]}...")
        time.sleep(0.2)  # Prevent throttling

    total_articles += len(articles)

print(f"✅ All {total_articles} articles sent to Kafka.")
producer.flush()