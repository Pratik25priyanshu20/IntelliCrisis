import os
from pymongo import MongoClient
from dotenv import load_dotenv

# Load environment variables
load_dotenv(dotenv_path="config/.env")
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")

# Define keyword lists
FIRE_KEYWORDS = [
    "Waldbrand", "Vegetationsbrand", "Flächenbrand", "Buschbrand", "Großbrand",
    "Feuer", "brennt", "Brandherd", "Löscheinsatz", "Feuerwehr", "Flammen", "Brandopfer",
    "Funkenflug", "Evakuierung", "Löschflugzeug", "Feueralarm", "Löscharbeiten"
]

LOCATION_TERMS = [
    "wald", "forst", "berg", "naturpark", "landkreis", "gebirge", "täler", "alpen",
    "schwarzwald", "bayern", "brandenburg", "niedersachsen", "rheinland", "sachsen", "thüringen",
    "mecklenburg", "hessen", "pfalz", "saarland", "hamburg", "berlin", "bremen"
]

# Connect to MongoDB
client = MongoClient(MONGO_URI)
db = client["disaster_db"]
news_collection = db["news_articles"]

print("🔎 Scanning articles for relevance...")

cursor = news_collection.find({"relevance_score": {"$exists": False}})
updated_count = 0

for doc in cursor:
    title = (doc.get("title") or "").lower()
    description = (doc.get("description") or "").lower()

    is_fire = any(kw in title or kw in description for kw in FIRE_KEYWORDS)
    is_location = any(loc in title or loc in description for loc in LOCATION_TERMS)

    if is_fire and is_location:
        score = 1.0
    elif is_fire or is_location:
        score = 0.5
    else:
        score = 0.0

    news_collection.update_one(
        {"_id": doc["_id"]},
        {"$set": {"relevance_score": score}}
    )
    updated_count += 1

print(f"✅ Updated {updated_count} articles with relevance scores.")