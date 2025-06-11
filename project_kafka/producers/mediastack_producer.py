# project_kafka/producers/mediastack_producer.py
import os
import json
import time
import requests
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable, KafkaError
from dotenv import load_dotenv
from datetime import datetime

def wait_for_kafka(broker, max_retries=10, retry_delay=5):
    """Wait for Kafka to be available with exponential backoff"""
    for attempt in range(max_retries):
        try:
            test_producer = KafkaProducer(
                bootstrap_servers=broker,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                api_version=(0, 10, 1),
                request_timeout_ms=10000,
                connections_max_idle_ms=30000
            )
            test_producer.close()
            print(f"✅ Successfully connected to Kafka at {broker}")
            return True
        except NoBrokersAvailable:
            wait_time = retry_delay * (2 ** attempt)
            print(f"⏳ Kafka not ready (attempt {attempt + 1}/{max_retries}). Waiting {wait_time}s...")
            time.sleep(wait_time)
        except Exception as e:
            print(f"❌ Unexpected error connecting to Kafka: {e}")
            time.sleep(retry_delay)
    
    raise Exception(f"Failed to connect to Kafka after {max_retries} attempts")

def run():
    """Main function to run MediaStack producer"""
    print("🌐 Starting MediaStack Kafka producer...")
    
    # Load environment variables
    load_dotenv("/opt/airflow/config/.env")
    MEDIASTACK_KEY = os.getenv("MEDIASTACK_KEY")
    KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:29092")
    TOPIC = os.getenv("KAFKA_MEDIASTACK_TOPIC", "mediastack_articles")
    
    # Validate API key
    if not MEDIASTACK_KEY:
        raise ValueError("MEDIASTACK_KEY not found in environment variables")
    
    print(f"📡 Connecting to Kafka broker: {KAFKA_BROKER}")
    print(f"📋 Target topic: {TOPIC}")
    
    # Wait for Kafka to be ready
    wait_for_kafka(KAFKA_BROKER)
    
    # Kafka producer setup
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        api_version=(0, 10, 1),
        request_timeout_ms=10000,
        connections_max_idle_ms=30000,
        retries=5,
        acks='all'
    )
    
    KEYWORDS = ["Waldbrand", "Vegetationsbrand", "Flächenbrand", "Feuer", "Erdrutsch", "landslide"]
    COUNTRIES = ["de", "ch"]  # Germany, Switzerland
    LANGUAGES = ["en", "de"]
    
    total_articles = 0
    
    try:
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
                    
                    try:
                        response = requests.get(url, timeout=10)
                        
                        if response.status_code == 200:
                            data = response.json()
                            articles = data.get("data", [])
                            print(f"📰 Articles found: {len(articles)}")
                            
                            for article in articles:
                                record = {
                                    "title": article.get("title", ""),
                                    "description": article.get("description", ""),
                                    "url": article.get("url", ""),
                                    "publishedAt": article.get("published_at", ""),
                                    "source": article.get("source", ""),
                                    "country": country,
                                    "language": lang,
                                    "query": keyword,
                                    "source_type": "mediastack",
                                    "timestamp": datetime.utcnow().isoformat()
                                }
                                
                                try:
                                    future = producer.send(TOPIC, value=record)
                                    future.get(timeout=10)
                                    total_articles += 1
                                    print(f"🚀 Sent article #{total_articles}: {record['title'][:60]}...")
                                except KafkaError as e:
                                    print(f"❌ Failed to send article to Kafka: {e}")
                                    
                        elif response.status_code == 429:
                            print(f"⚠️ Rate limited - waiting 60s")
                            time.sleep(60)
                        else:
                            print(f"❌ Error {response.status_code}: {response.text}")
                            
                    except requests.RequestException as e:
                        print(f"❌ Failed to fetch MediaStack articles: {e}")
                    except Exception as e:
                        print(f"❌ Unexpected error: {e}")
                    
                    # Delay between requests to avoid rate limiting
                    time.sleep(1)
                    
    finally:
        print(f"✅ MediaStack producer finished. Total articles sent: {total_articles}")
        producer.flush()
        producer.close()

if __name__ == "__main__":
    run()