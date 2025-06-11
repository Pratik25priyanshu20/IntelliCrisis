#mongo_handler.py
from pymongo import MongoClient
import os
from dotenv import load_dotenv

load_dotenv("config/.env")

def store_to_mongo(record, collection_obj, topic):
    try:
        print(f"Storing to MongoDB into '{collection_obj.name}' from topic '{topic}': {record.get('title', 'No title')}")
        collection_obj.insert_one(record)
        print(f"✅ Successfully stored message from topic [{topic}] into '{collection_obj.name}'")
        return True
    except Exception as e:
        print(f"❌ MongoDB insert error for topic '{topic}': {e}")
        return False

def close_mongo_connection(client_instance):
    if client_instance:
        client_instance.close()
        print("MongoDB connection closed.")