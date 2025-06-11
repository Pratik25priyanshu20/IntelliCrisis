'''
import os
import json
import time
import requests
from datetime import datetime, timedelta
from dotenv import load_dotenv
from project_kafka import KafkaProducer

# Load environment variables
load_dotenv("config/.env")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC_NASA", "nasa_firms")
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
NASA_API_URL = "https://firms.modaps.eosdis.nasa.gov/api/country/csv"  # Example endpoint

# Setup Kafka producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

def fetch_mock_firms_data():
    # Replace with NASA FIRMS actual API logic or local CSV parsing as needed
    mock_data = [
        {
            "latitude": 46.316,
            "longitude": 7.816,
            "acq_date": datetime.utcnow().strftime("%Y-%m-%d"),
            "confidence": 85,
            "disaster_type": "wildfire",
            "source": "NASA FIRMS",
            "source_type": "nasa_firms",
            "timestamp": datetime.utcnow().isoformat()
        },
        # Add more mock entries or CSV-parsed data
    ]
    return mock_data

def main():
    print("🚀 Fetching NASA FIRMS data...")
    records = fetch_mock_firms_data()
    for record in records:
        producer.send(KAFKA_TOPIC, value=record)
        print(f"📤 Sent: {record}")
        time.sleep(0.1)
    producer.flush()
    print(f"✅ {len(records)} NASA FIRMS records sent to Kafka")

if __name__ == "__main__":
    main()
    
'''

import os
import sys
sys.path.append('/opt/airflow/project_kafka')  # 🔧 Ensures KafkaProducer is found

import json
import time
import requests
from datetime import datetime, timedelta
from dotenv import load_dotenv
from kafka import KafkaProducer  # ✅ Will work now

# Load environment variables
load_dotenv("config/.env")

KAFKA_TOPIC = os.getenv("KAFKA_TOPIC_NASA", "nasa_firms")
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")

# NASA FIRMS API (placeholder)
NASA_API_URL = "https://firms.modaps.eosdis.nasa.gov/api/country/csv"

# Setup Kafka producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

def fetch_mock_firms_data():
    # Replace this with real API logic or local CSV parsing
    mock_data = [
        {
            "latitude": 46.316,
            "longitude": 7.816,
            "acq_date": datetime.utcnow().strftime("%Y-%m-%d"),
            "confidence": 85,
            "disaster_type": "wildfire",
            "source": "NASA FIRMS",
            "source_type": "nasa_firms",
            "timestamp": datetime.utcnow().isoformat()
        }
    ]
    return mock_data

def main():
    print("🚀 Fetching NASA FIRMS data...")
    records = fetch_mock_firms_data()
    for record in records:
        producer.send(KAFKA_TOPIC, value=record)
        print(f"📤 Sent: {record}")
        time.sleep(0.1)
    producer.flush()
    print(f"✅ {len(records)} NASA FIRMS records sent to Kafka")

if __name__ == "__main__":
    main()