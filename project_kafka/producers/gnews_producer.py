# kafka/producers/gnews_producer.py
import os
import json
import requests
import time
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable, KafkaError
from datetime import datetime
from dotenv import load_dotenv

def wait_for_kafka(broker, max_retries=10, retry_delay=5):
    """Wait for Kafka to be available with exponential backoff"""
    for attempt in range(max_retries):
        try:
            # Try to create a producer to test connection
            test_producer = KafkaProducer(
                bootstrap_servers=broker,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                api_version=(0, 10, 1),  # Specify API version
                request_timeout_ms=10000,
                connections_max_idle_ms=30000
            )
            test_producer.close()
            print(f"✅ Successfully connected to Kafka at {broker}")
            return True
        except NoBrokersAvailable:
            wait_time = retry_delay * (2 ** attempt)  # Exponential backoff
            print(f"⏳ Kafka not ready (attempt {attempt + 1}/{max_retries}). Waiting {wait_time}s...")
            time.sleep(wait_time)
        except Exception as e:
            print(f"❌ Unexpected error connecting to Kafka: {e}")
            time.sleep(retry_delay)
    
    raise Exception(f"Failed to connect to Kafka after {max_retries} attempts")

def run():
    """Main function to run GNews producer"""
    print("🚀 Starting GNews producer...")
    
    # Load environment
    load_dotenv("/opt/airflow/config/.env")
    GNEWS_API_KEY = os.getenv("GNEWS_API_KEY")
    KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:29092")  # Use internal port
    KAFKA_TOPIC = os.getenv("KAFKA_GNEWS_TOPIC", "gnews_articles")
    
    # Validate API key
    if not GNEWS_API_KEY:
        raise ValueError("GNEWS_API_KEY not found in environment variables")
    
    print(f"📡 Connecting to Kafka broker: {KAFKA_BROKER}")
    print(f"📋 Target topic: {KAFKA_TOPIC}")
    
    # Wait for Kafka to be ready
    wait_for_kafka(KAFKA_BROKER)
    
    # Create producer with robust settings
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        api_version=(0, 10, 1),
        request_timeout_ms=10000,
        connections_max_idle_ms=30000,
        retries=5,
        acks='all'  # Wait for all replicas to acknowledge
    )
    
    DISASTER_KEYWORDS = [
        "wildfire", "landslide", "Waldbrand", "Erdrutsch",
        "Flächenbrand", "Bergsturz", "Valais", "Blatten"
    ]
    
    COUNTRIES = ["Germany", "Switzerland", "Deutschland", "Schweiz"]
    LANGUAGES = ["en", "de"]
    
    total_articles = 0
    
    try:
        for keyword in DISASTER_KEYWORDS:
            for country in COUNTRIES:
                for lang in LANGUAGES:
                    query = f"{keyword} {country}"
                    url = f"https://gnews.io/api/v4/search?q={query}&lang={lang}&token={GNEWS_API_KEY}"
                    
                    try:
                        print(f"🔍 Fetching: {query} ({lang})")
                        response = requests.get(url, timeout=10)
                        
                        if response.status_code == 200:
                            articles = response.json().get("articles", [])
                            print(f"📰 Found {len(articles)} articles for '{query}' in {lang}")
                            
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
                                
                                # Send to Kafka with error handling
                                try:
                                    future = producer.send(KAFKA_TOPIC, value=article_record)
                                    future.get(timeout=10)  # Wait for acknowledgment
                                    total_articles += 1
                                    print(f"📤 Sent article #{total_articles}: {article_record['title'][:60]}")
                                except KafkaError as e:
                                    print(f"❌ Failed to send article to Kafka: {e}")
                                    
                        elif response.status_code == 429:
                            print(f"⚠️ Rate limited for query '{query}' - waiting 60s")
                            time.sleep(60)
                        else:
                            print(f"❌ Error {response.status_code} for '{query}': {response.text}")
                            
                    except requests.RequestException as e:
                        print(f"❌ Failed to fetch GNews articles for '{query}': {e}")
                    except Exception as e:
                        print(f"❌ Unexpected error processing '{query}': {e}")
                    
                    # Small delay between requests to avoid rate limiting
                    time.sleep(1)
    
    finally:
        print(f"✅ Finished processing. Total articles sent: {total_articles}")
        producer.flush()
        producer.close()

# For backward compatibility - if run directly
if __name__ == "__main__":
    run()