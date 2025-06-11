#gnews_ingestor.py

import requests
from datetime import datetime, timedelta
import logging
from pymongo import MongoClient
import time

# --- Configuration ---
GNEWS_API_KEY = "8e96456dd9db99ee6a470919090b37a9"
MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "disaster_news"
COLLECTION_NAME = "gnews_articles"
COUNTRIES = ["Germany", "Switzerland"]
DISASTER_KEYWORDS = [
    ("wildfire", ["wildfire", "Waldbrand"]),
    ("landslide", ["landslide", "Erdrutsch"])
]
LANGUAGES = ["en", "de"]
DAYS_BACK = 5

# --- Setup Logging ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_mongo_collection():
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    return db[COLLECTION_NAME]

def fetch_articles(query, lang):
    base_url = "https://gnews.io/api/v4/search"
    end_date = datetime.utcnow()
    start_date = end_date - timedelta(days=DAYS_BACK)

    params = {
        "q": query,
        "lang": lang,
        "from": start_date.strftime('%Y-%m-%d'),
        "to": end_date.strftime('%Y-%m-%d'),
        "token": GNEWS_API_KEY,
        "sortby": "publishedAt",
        "max": 50
    }

    try:
        response = requests.get(base_url, params=params, timeout=15)
        if response.status_code == 200:
            return response.json().get("articles", [])
        else:
            logging.warning(f"❌ API error for '{query}' [{lang}]: {response.status_code}")
            return []
    except Exception as e:
        logging.error(f"❌ Exception for '{query}' [{lang}]: {e}")
        return []

def insert_articles(articles, disaster_type, country):
    collection = get_mongo_collection()
    inserted_count = 0

    for article in articles:
        entry = {
            "title": article.get("title"),
            "url": article.get("url"),
            "publishedAt": article.get("publishedAt"),
            "content": article.get("content"),
            "disaster_type": disaster_type,
            "country": country,
            "source": "GNews"
        }

        if not collection.find_one({"url": entry["url"]}):
            collection.insert_one(entry)
            inserted_count += 1

    return inserted_count

def run_ingestion():
    logging.info("🚀 Starting GNews ingestion...")
    total_inserted = 0

    for disaster_type, keyword_variants in DISASTER_KEYWORDS:
        for country in COUNTRIES:
            for keyword in keyword_variants:
                for lang in LANGUAGES:
                    query = f"{keyword} {country}"
                    logging.info(f"🔍 Searching: '{query}' in {lang}")
                    articles = fetch_articles(query, lang)
                    inserted = insert_articles(articles, disaster_type, country)
                    logging.info(f"📰 Inserted {inserted} new articles for '{query}' [{lang}]")
                    total_inserted += inserted
                    time.sleep(1.2)

    logging.info(f"✅ News ingestion complete. Total articles inserted: {total_inserted}")

if __name__ == "__main__":
    run_ingestion()
