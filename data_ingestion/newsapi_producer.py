import os
import requests
from datetime import datetime, timedelta
from pymongo import MongoClient
from dotenv import load_dotenv
import time
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Disaster-related keywords
DISASTER_KEYWORDS = {
    "wildfire": [
        "Waldbrand", "Vegetationsbrand", "Flächenbrand", "Buschbrand", "Großbrand",
        "Feuer", "brennt", "Brandherd", "Löscheinsatz", "Feuerwehr", "Flammen", "Brandopfer",
        "Funkenflug", "Evakuierung", "Löschflugzeug", "Feueralarm", "Löscharbeiten",
        "wildfire", "forest fire", "bushfire", "firefighting", "burning"
    ],
    "landslide": [
        "Erdrutsch", "Hangrutschung", "Bergsturz", "Murenabgang", "Schlammlawine",
        "Hangbewegung", "Gerölllawine", "Hangrutsch", "Erdrutsche", "Lawine", "Felssturz",
        "landslide", "rockfall", "mudslide", "debris flow", "mass movement"
    ]
}

LOCATION_TERMS = [
    "Germany", "Deutschland", "Switzerland", "Schweiz", "Blatten", "Valais"
]

# Load environment
load_dotenv("config/.env")
NEWSAPI_KEY = os.getenv("NEWSAPI_KEY")
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")

# MongoDB setup
client = MongoClient(MONGO_URI)
db = client["disaster_db"]
news_collection = db["news_articles"]
fire_collection = db["wildfires"]
landslide_collection = db["landslides"]

logger.info("🚀 Starting News API ingestion...")

cutoff_date = (datetime.utcnow() - timedelta(days=30)).strftime("%Y-%m-%d")
fires = list(fire_collection.find({"acq_date": {"$gte": cutoff_date}}))
landslides = list(landslide_collection.find({"date": {"$gte": cutoff_date}}))

logger.info(f"📦 Found {len(fires)} wildfire records for DEU")
logger.info(f"📦 Found {len(fires)} wildfire records for CHE")
logger.info(f"📦 Found {len(landslides)} landslide records for CHE")
logger.info(f"📦 Found {len(landslides)} landslide records for DEU")

events = [(f, "wildfire") for f in fires] + [(l, "landslide") for l in landslides]
inserted_total = 0

for event, dtype in events:
    lat = event.get("latitude")
    lon = event.get("longitude")
    date = event.get("acq_date") if dtype == "wildfire" else event.get("date")
    
    query_keywords = " OR ".join(DISASTER_KEYWORDS[dtype])
    location_terms = " OR ".join(LOCATION_TERMS)
    query = f"({query_keywords}) AND ({location_terms})"

    from_date = (datetime.strptime(date, "%Y-%m-%d") - timedelta(days=5)).strftime("%Y-%m-%d")
    to_date = (datetime.strptime(date, "%Y-%m-%d") + timedelta(days=5)).strftime("%Y-%m-%d")

    url = (
        f"https://newsapi.org/v2/everything?q={query}&from={from_date}&to={to_date}"
        f"&sortBy=publishedAt&language=de,en&apiKey={NEWSAPI_KEY}"
    )

    logger.info(f"🔍 Fetching {dtype} news for {lat},{lon} on {date}")
    response = requests.get(url)
    if response.status_code != 200:
        logger.warning(f"❌ Request failed: {response.status_code} - {response.text}")
        continue

    articles = response.json().get("articles", [])
    inserted = 0
    for article in articles:
        title = article.get("title", "")
        description = article.get("description", "")
        if not title and not description:
            continue

        title_lower = title.lower()
        desc_lower = description.lower()
        keywords = DISASTER_KEYWORDS[dtype]
        if not any(kw.lower() in title_lower or kw.lower() in desc_lower for kw in keywords):
            continue

        article_record = {
            "linked_disaster_type": dtype,
            "linked_disaster_id": event["_id"],
            "title": title,
            "description": description,
            "url": article.get("url"),
            "publishedAt": article.get("publishedAt"),
            "source": article.get("source", {}).get("name", ""),
            "source_type": "newsapi",
            "query_lat": lat,
            "query_lon": lon,
            "query_date": date,
            "timestamp": datetime.utcnow()
        }

        if news_collection.count_documents({"url": article_record["url"]}) == 0:
            news_collection.insert_one(article_record)
            inserted += 1
            logger.info(f"📰 {article_record['publishedAt']} | {article_record['source']}: {title}")

    logger.info(f"🗞️ Inserted {inserted} articles for {dtype} on {date}")
    inserted_total += inserted
    time.sleep(1)

logger.info(f"✅ News ingestion complete. Total articles inserted: {inserted_total}")
