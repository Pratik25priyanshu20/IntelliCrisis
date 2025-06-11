#unified_consumer.py
import os
import json
import logging
import signal
import sys
import datetime 
from kafka import KafkaConsumer, KafkaProducer
from dotenv import load_dotenv
from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import List, Optional
from pymongo import MongoClient

from storage.mongo_handler import store_to_mongo, close_mongo_connection

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class ConsumerSettings(BaseSettings):
    model_config = SettingsConfigDict(env_file="config/.env", extra="ignore")

    KAFKA_BOOTSTRAP_SERVERS: str = "localhost:9092"
    KAFKA_TOPICS_STR: str = "newsapi_articles,gnews_articles,reddit_articles,mediastack_articles,googlerss_articles,nasa_firms,test_articles"
    KAFKA_TOPICS: List[str] = []

    STORAGE_BACKEND: str = "mongo"
    KAFKA_DLQ_SUFFIX: str = "_dlq"
    KAFKA_CONSUMER_GROUP_ID: str = "disaster-news-consumer-group"
    MONGO_URI: Optional[str] = None

    KAFKA_TOPIC_TO_COLLECTION_MAP: dict = {
        "newsapi_articles": "news_articles",
        "gnews_articles": "news_articles",
        "reddit_articles": "news_articles",
        "mediastack_articles": "news_articles",
        "googlerss_articles": "news_articles",
        "nasa_firms": "firms_data",
        "nasa_landslide": "landslide_data",
        "test_articles": "news_articles",  # map test topic to default collection
    }

    def model_post_init(self, __context: any) -> None:
        self.KAFKA_TOPICS = [topic.strip() for topic in self.KAFKA_TOPICS_STR.split(",")]
        del self.KAFKA_TOPICS_STR

class UnifiedConsumer:
    def __init__(self, settings: ConsumerSettings):
        self.settings = settings
        self.running = True

        self.producer = self._create_kafka_producer()
        self.consumer = self._create_kafka_consumer()

        if self.settings.STORAGE_BACKEND == "mongo":
            self.mongo_client = self._create_mongo_client()
            self.db = self.mongo_client["disaster_db"]
            logger.info(f"MongoDB URI: {self.settings.MONGO_URI}")
        else:
            raise ValueError(f"Unsupported storage backend: {self.settings.STORAGE_BACKEND}")

        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

        logger.info(f"🔌 Connecting to Kafka at {self.settings.KAFKA_BOOTSTRAP_SERVERS} and listening to topics: {self.settings.KAFKA_TOPICS}")
        logger.info(f"Using storage backend: {self.settings.STORAGE_BACKEND}")
        logger.info(f"Kafka topic to MongoDB collection map: {self.settings.KAFKA_TOPIC_TO_COLLECTION_MAP}")

    def _create_kafka_producer(self) -> KafkaProducer:
        logger.info(f"Creating Kafka Producer for bootstrap servers: {self.settings.KAFKA_BOOTSTRAP_SERVERS}")
        return KafkaProducer(
            bootstrap_servers=self.settings.KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            max_block_ms=60000,
            api_version=(0, 10, 2)
        )

    def _safe_json_deserializer(self, message_bytes):
        if message_bytes is None:
            logger.warning("Received None message_bytes, returning None.")
            return None
        try:
            return json.loads(message_bytes.decode('utf-8'))
        except json.JSONDecodeError:
            logger.error(f"Could not deserialize message as JSON. Raw bytes: {message_bytes!r}. Skipping JSON processing for this message.")
            try:
                return message_bytes.decode('utf-8')
            except UnicodeDecodeError:
                logger.error(f"Could not decode message as UTF-8. Raw bytes: {message_bytes!r}. Returning None.")
                return None
        except Exception as e:
            logger.error(f"Unexpected error during deserialization: {e}. Raw bytes: {message_bytes!r}. Returning None.", exc_info=True)
            return None

    def _create_kafka_consumer(self) -> KafkaConsumer:
        logger.info(f"Creating Kafka Consumer for topics: {self.settings.KAFKA_TOPICS} with group ID: {self.settings.KAFKA_CONSUMER_GROUP_ID}")
        consumer = KafkaConsumer(
            *self.settings.KAFKA_TOPICS,
            bootstrap_servers=self.settings.KAFKA_BOOTSTRAP_SERVERS,
            auto_offset_reset="earliest",
            enable_auto_commit=False,
            group_id=self.settings.KAFKA_CONSUMER_GROUP_ID,
            value_deserializer=self._safe_json_deserializer
        )
        return consumer

    def _create_mongo_client(self) -> MongoClient:
        if not self.settings.MONGO_URI:
            default_uri = "mongodb://localhost:27017/disaster_db"
            logger.warning(f"MONGO_URI not set, using default: {default_uri}")
            return MongoClient(default_uri)
        logger.info(f"Connecting to MongoDB at {self.settings.MONGO_URI}")
        return MongoClient(self.settings.MONGO_URI)

    def _signal_handler(self, signum, frame):
        logger.info(f"Signal {signum} received, initiating graceful shutdown...")
        self.running = False

    def run(self):
        logger.info("Starting Kafka consumer loop...")
        logger.debug("UnifiedConsumer run method entered.") 
        try:
            for message in self.consumer:
                if not self.running:
                    break

                data = message.value
                topic = message.topic

                if data is None:
                    logger.warning(f"Skipping message from topic '{topic}' at offset {message.offset} due to deserialization error (data is None).")
                    self._send_to_dlq(message.value, "deserialization_null", topic) # Send to DLQ even if data is None
                    self.consumer.commit()
                    continue

                if not isinstance(data, dict):
                    logger.warning(f"Received non-JSON message from topic '{topic}' at offset {message.offset}: {data!r}. Sending to DLQ.")
                    self._send_to_dlq(message.value, "non_json_format", topic)
                    self.consumer.commit()
                    continue

                try:
                    logger.info(f"Received message from topic '{topic}': {data.get('title', 'No Title')}")

                    logger.debug(f"About to call store_to_mongo with data title: {data.get('title', 'N/A')}")

                    if self.settings.STORAGE_BACKEND == "mongo":
                        coll_name = self.settings.KAFKA_TOPIC_TO_COLLECTION_MAP.get(topic, "news_articles")
                        collection = self.db[coll_name]
                        store_to_mongo(data, collection, topic)

                    self.consumer.commit()

                except Exception as e:
                    logger.error(f"Error processing valid message from topic '{message.topic}': {e}", exc_info=True)
                    self._send_to_dlq(message.value, "processing_error", message.topic)
                    self.consumer.commit()

        except Exception as e:
            logger.critical(f"Kafka consumer critical error: {e}", exc_info=True)
        finally:
            self.shutdown()

    def _send_to_dlq(self, message_value, error_type, original_topic):
        dlq_topic = f"{original_topic}{self.settings.KAFKA_DLQ_SUFFIX}"
        try:
            if isinstance(message_value, bytes):
                dlq_original_message = message_value.decode('utf-8', errors='ignore')
            elif message_value is None:
                dlq_original_message = "NULL_MESSAGE_VALUE"
            else:
                dlq_original_message = message_value

            dlq_message = {
                "original_message": dlq_original_message,
                "error_type": error_type,
                "timestamp": datetime.datetime.utcnow().isoformat() + "Z" 
            }
            self.producer.send(dlq_topic, dlq_message)
            logger.warning(f"Sent message to DLQ topic '{dlq_topic}' due to {error_type} error.")
        except Exception as e:
            logger.error(f"Failed to send message to DLQ: {e}", exc_info=True)

    def shutdown(self):
        logger.info("Shutting down Kafka consumer and producer...")
        if self.consumer:
            self.consumer.close()
        if self.producer:
            self.producer.close()
        if hasattr(self, 'mongo_client') and self.mongo_client:
            close_mongo_connection(self.mongo_client)
        logger.info("Shutdown complete.")
        sys.exit(0)


if __name__ == "__main__":
    load_dotenv("config/.env")
    settings = ConsumerSettings()
    os.environ["MONGO_URI"] = settings.MONGO_URI or os.getenv("MONGO_URI", "mongodb://localhost:27017/disaster_db")

    try:
        consumer_app = UnifiedConsumer(settings)
        consumer_app.run()
    except Exception as e:
        logger.critical(f"Application failed to start: {e}", exc_info=True)
        sys.exit(1)