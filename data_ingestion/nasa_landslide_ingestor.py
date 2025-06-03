# data_ingestion/nasa_landslide_ingestor.py

import requests
from pymongo import MongoClient
from datetime import datetime
import os
from dotenv import load_dotenv

load_dotenv("config/.env")
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
client = MongoClient(MONGO_URI)
db = client["disaster_db"]
landslide_collection = db["landslides"]

print("🌍 Fetching NASA Global Landslide Catalog data...")

url = "https://data.nasa.gov/resource/dd9e-wu2v.json?$limit=1000"

try:
    response = requests.get(url)
    if response.status_code != 200:
        print(f"❌ Failed to fetch: {response.status_code} - {response.text}")
        exit()

    data = response.json()
    inserted = 0
    for item in data:
        if not item.get("event_id") or not item.get("latitude") or not item.get("longitude"):
            continue

        record = {
            "type": "landslide",
            "event_id": item.get("event_id"),
            "location": item.get("location"),
            "date": item.get("date"),
            "latitude": float(item.get("latitude")),
            "longitude": float(item.get("longitude")),
            "source_name": item.get("source_name"),
            "fatality_count": item.get("fatality_count"),
            "landslide_size": item.get("landslide_size"),
            "trigger": item.get("trigger"),
            "created_at": datetime.utcnow()
        }

        if landslide_collection.count_documents({"event_id": record["event_id"]}) == 0:
            landslide_collection.insert_one(record)
            inserted += 1

    print(f"✅ Inserted {inserted} new landslide records.")

except Exception as e:
    print(f"❌ Error: {e}")