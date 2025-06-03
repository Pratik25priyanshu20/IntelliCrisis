from pymongo import MongoClient
from datetime import datetime
import os
from dotenv import load_dotenv

load_dotenv("config/.env")

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
client = MongoClient(MONGO_URI)
db = client["disaster_db"]
collection = db["news_articles"]

def store_to_mongo(message):
    try:
        message["timestamp"] = datetime.utcnow()
        collection.insert_one(message)
        print(f"✅ Stored in MongoDB: {message.get('title')[:80]}")
    except Exception as e:
        print(f"❌ MongoDB insert error: {e}")