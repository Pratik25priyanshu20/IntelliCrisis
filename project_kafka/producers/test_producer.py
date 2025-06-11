# test_producer.py
# project_kafka/producers/test_producer.py
import os
import json
import time
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
from datetime import datetime
from dotenv import load_dotenv

def run_test_producer():
    dotenv_path = "/opt/airflow/config/.env"
    if not os.path.exists(dotenv_path):
        print(f"❌ .env file not found at {dotenv_path}")
    else:
        load_dotenv(dotenv_path)
        print(f"✅ .env file loaded from {dotenv_path}")

    KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:29092")
    TEST_TOPIC = os.getenv("KAFKA_TEST_TOPIC", "test_articles")

    print(f"DEBUG: KAFKA_BROKER read: {KAFKA_BROKER}")
    print(f"DEBUG: TEST_TOPIC read: {TEST_TOPIC}")

    producer = None
    # Wait for Kafka to be available
    for i in range(10):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                api_version=(0, 10, 1),
                request_timeout_ms=10000
            )
            print("✅ Kafka Producer connected.")
            break
        except NoBrokersAvailable:
            print(f"⏳ Kafka not ready (attempt {i+1}/10). Waiting 5s...")
            time.sleep(5)
        except Exception as e:
            print(f"❌ Unexpected error connecting to Kafka: {e}")
            time.sleep(5)

    if not producer:
        raise Exception("Failed to connect to Kafka producer.")

    try:
        for i in range(3): # Send 3 sample messages
            test_message = {
                "title": f"Test Article {i+1} - Flood in Germany",
                "description": f"This is a sample description for test article {i+1} about a flood event.",
                "url": f"http://test.com/article/{i+1}",
                "publishedAt": datetime.utcnow().isoformat() + "Z",
                "source": "TestProducer",
                "country": "de",
                "language": "en",
                "query": "test_flood",
                "source_type": "test_producer",
                "timestamp": datetime.utcnow().isoformat()
            }
            future = producer.send(TEST_TOPIC, value=test_message)
            record_metadata = future.get(timeout=10)
            print(f"🚀 Sent test message #{i+1} to topic {record_metadata.topic}, partition {record_metadata.partition}, offset {record_metadata.offset}")
            time.sleep(1) # Small delay

    except Exception as e:
        print(f"❌ Error sending test message: {e}")
    finally:
        if producer:
            producer.flush()
            producer.close()
            print("✅ Test Producer finished and closed.")

if __name__ == "__main__":
    run_test_producer()