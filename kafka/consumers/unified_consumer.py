import os
import json
from kafka import KafkaConsumer
from dotenv import load_dotenv

# Storage backends
from storage.mongo_handler import store_to_mongo
from storage.delta_handler import store_to_delta

# Load environment variables
load_dotenv("config/.env")

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPICS = os.getenv("KAFKA_TOPICS", "newsapi,gnews,reddit,mediastack,googlerss").split(",")
STORAGE_BACKEND = os.getenv("STORAGE_BACKEND", "mongo")  # "mongo" or "delta"

print(f"🔌 Connecting to Kafka at {KAFKA_BOOTSTRAP_SERVERS} and listening to topics: {KAFKA_TOPICS}")

# Create Kafka consumer
consumer = KafkaConsumer(
    *KAFKA_TOPICS,
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="disaster-news-consumer",
    value_deserializer=lambda m: json.loads(m.decode("utf-8"))
)

print(f"💾 Storage backend selected: {STORAGE_BACKEND}")

# Message processing loop
for message in consumer:
    try:
        record = message.value
        topic = message.topic

        print(f"📨 Received message from topic [{topic}]")

        # Store based on backend
        if STORAGE_BACKEND == "mongo":
            store_to_mongo(record, topic)
        elif STORAGE_BACKEND == "delta":
            store_to_delta(record, topic)
        else:
            print(f"⚠️ Unknown storage backend: {STORAGE_BACKEND}")

    except Exception as e:
        print(f"❌ Error processing message: {e}")
