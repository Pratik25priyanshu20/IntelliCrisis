# data_ingestion/google_rss_ingestor.py

import os
import requests
from bs4 import BeautifulSoup
from datetime import datetime
from pymongo import MongoClient
from dotenv import load_dotenv
import logging
import time

# Logging setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Load environment
load_dotenv("config/.env")
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")

# MongoDB setup
client = MongoClient(MONGO_URI)
db = client["disaster_db"]
collection = db["news_articles"]

# Disaster keywords and country/language settings
DISASTER_KEYWORDS = {
    "wildfire": ["wildfire", "Waldbrand"],
    "landslide": ["landslide", "Erdrutsch"]
}
COUNTRIES = ["Germany", "Deutschland", "Switzerland", "Schweiz"]
LANGUAGES = ["en", "de"]  # language= parameter in Google News RSS is unreliable but we still loop for completeness

BASE_URL = "https://news.google.com/rss/search"

def fetch_articles(query: str):
    params = {
        "q": query,
        "hl": "en",  # hl doesn't guarantee language filtering, but we still try
        "gl": "US",
        "ceid": "US:en"
    }
    full_url = f"{BASE_URL}?q={requests.utils.quote(query)}"

    try:
        response = requests.get(full_url, timeout=15)
        if response.status_code != 200:
            logging.warning(f"❌ Failed to fetch for query: {query} | {response.status_code}")
            return []

        soup = BeautifulSoup(response.content, features="xml")
        return soup.find_all("item")

    except Exception as e:
        logging.error(f"❌ Error fetching for query: {query} | {str(e)}")
        return []

def run_ingestion():
    total_inserted = 0
    logging.info("🚀 Starting Google RSS ingestion...")

    for dtype, keywords in DISASTER_KEYWORDS.items():
        for keyword in keywords:
            for country in COUNTRIES:
                query = f"{keyword} {country}"
                logging.info(f"🔍 Searching: {query}")
                items = fetch_articles(query)

                inserted = 0
                for item in items:
                    title = item.title.text
                    link = item.link.text
                    description = item.description.text.strip()
                    pub_date = item.pubDate.text.strip()
                    published_at = datetime.strptime(pub_date, "%a, %d %b %Y %H:%M:%S %Z")

                    if collection.count_documents({"url": link}) == 0:
                        doc = {
                            "title": title,
                            "description": description,
                            "url": link,
                            "publishedAt": published_at,
                            "source": "Google News RSS",
                            "source_type": "google_rss",
                            "linked_disaster_type": dtype,
                            "timestamp": datetime.utcnow()
                        }
                        collection.insert_one(doc)
                        inserted += 1

                logging.info(f"🗞️ Inserted {inserted} articles for query '{query}'")
                total_inserted += inserted
                time.sleep(1)  # avoid aggressive hits to Google

    logging.info(f"✅ Google RSS ingestion complete. Total articles inserted: {total_inserted}")

if __name__ == "__main__":
    run_ingestion()