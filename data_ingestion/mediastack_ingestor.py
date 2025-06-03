# data_ingestion/mediastack_ingestor.py

import os
import requests
from datetime import datetime
from pymongo import MongoClient
from dotenv import load_dotenv
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Load environment
load_dotenv("config/.env")
MEDIASTACK_KEY = os.getenv("MEDIASTACK_KEY")
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")

# MongoDB connection
client = MongoClient(MONGO_URI)
db = client["disaster_db"]
news_collection = db["news_articles"]

# Keywords and parameters
DISASTER_KEYWORDS = {
    "wildfire": ["Waldbrand", "Vegetationsbrand", "Flächenbrand", "wildfire", "forest fire", "Feuer"],
    "landslide": ["Erdrutsch", "Hangrutsch", "landslide", "mudslide", "rockfall", "Bergsturz"]
}
LANGUAGES = ["en", "de"]
COUNTRIES = ["de", "ch"]  # DE = Germany, CH = Switzerland
LIMIT = 50

logger.info("🚀 Starting Mediastack ingestion...")

inserted_total = 0

for dtype, keywords in DISASTER_KEYWORDS.items():
    for keyword in keywords:
        for lang in LANGUAGES:
            for country in COUNTRIES:
                logger.info(f"🔍 Searching: '{keyword}' in {country.upper()} [{lang}]")

                url = (
                    f"http://api.mediastack.com/v1/news?"
                    f"access_key={MEDIASTACK_KEY}&"
                    f"keywords={keyword}&"
                    f"languages={lang}&"
                    f"countries={country}&"
                    f"limit={LIMIT}"
                )

                response = requests.get(url)
                if response.status_code != 200:
                    logger.warning(f"❌ Request failed: {response.status_code} - {response.text}")
                    continue

                articles = response.json().get("data", [])
                inserted = 0

                for article in articles:
                    title = article.get("title")
                    description = article.get("description")
                    url_link = article.get("url")
                    if not title or not url_link:
                        continue

                    if news_collection.count_documents({"url": url_link}) == 0:
                        news_collection.insert_one({
                            "title": title,
                            "description": description,
                            "url": url_link,
                            "publishedAt": article.get("published_at"),
                            "source": article.get("source"),
                            "source_type": "mediastack",
                            "language": lang,
                            "country": country.upper(),
                            "linked_disaster_type": dtype,
                            "timestamp": datetime.utcnow()
                        })
                        inserted += 1

                logger.info(f"🗞️ Inserted {inserted} articles for '{keyword}' in {country.upper()} [{lang}]")
                inserted_total += inserted

logger.info(f"✅ Mediastack ingestion complete. Total articles inserted: {inserted_total}")