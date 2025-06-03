import pandas as pd
import requests
from pymongo import MongoClient
from datetime import datetime
import os
from dotenv import load_dotenv
from io import StringIO

# Load config
load_dotenv(dotenv_path="config/.env")
NASA_KEY = os.getenv("NASA_MAP_KEY")
NASA_URL = os.getenv("NASA_FIRMS_URL")
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")

print("🔍 DEBUG: NASA_MAP_KEY =", os.getenv("NASA_MAP_KEY"))
print("🔍 DEBUG: NASA_FIRMS_URL =", os.getenv("NASA_FIRMS_URL"))

if not NASA_KEY or not NASA_URL:
    raise ValueError("❌ NASA_KEY or NASA_URL is missing. Check your .env file and ensure keys are correct.")

# Build request URL for Germany
DAYS = os.getenv("FIRMS_DAYS", "3")
endpoint = f"{NASA_URL}/{NASA_KEY}/VIIRS_SNPP_NRT/DEU/{DAYS}"
print(f"📡 Fetching from: {endpoint}")

# MongoDB setup
client = MongoClient(MONGO_URI)
db = client["disaster_db"]
collection = db["wildfires"]

# Fetch and read CSV
response = requests.get(endpoint)
if response.status_code != 200:
    print("❌ Failed to fetch data:", response.status_code)
    exit()

df = pd.read_csv(StringIO(response.text))
print(f"✅ Retrieved {len(df)} records")

# Normalize and insert
records = []
for _, row in df.iterrows():
    records.append({
        "country": row.get("country_id", "DEU"),
        "latitude": row.get("latitude"),
        "longitude": row.get("longitude"),
        "brightness": row.get("bright_ti4"),
        "confidence": row.get("confidence"),
        "acq_date": row.get("acq_date"),
        "acq_time": row.get("acq_time"),
        "satellite": row.get("satellite"),
        "instrument": row.get("instrument"),
        "frp": row.get("frp"),
        "daynight": row.get("daynight"),
        "source": "NASA_FIRMS",
        "timestamp": datetime.utcnow()
    })

# Insert to MongoDB
if records:
    collection.insert_many(records)
    print(f"🟢 Inserted {len(records)} wildfire records into MongoDB.")
else:
    print("⚠️ No records to insert.")