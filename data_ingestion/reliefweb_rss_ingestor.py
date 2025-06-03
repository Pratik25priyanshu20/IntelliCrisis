import os
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta, timezone
from pymongo import MongoClient
from dotenv import load_dotenv

# Load environment
load_dotenv("config/.env")
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")

# MongoDB setup
client = MongoClient(MONGO_URI)
db = client["disaster_db"]
collection = db["news_articles"]

# Keywords
DISASTER_KEYWORDS = {
    "wildfire": [
        "wildfire", "forest fire", "bushfire", "vegetation fire", "Waldbrand", "Buschbrand", "Feuer", "Flammen"
    ],
    "landslide": [
        "landslide", "mudslide", "debris flow", "rockfall", "Erdrutsch", "Schlammlawine", "Bergsturz", "Murenabgang"
    ]
}

LOCATION_TERMS = [
    "Germany", "Deutschland", "Switzerland", "Schweiz", "Blatten", "Wallis", "Bern", "Valais",
    "Europe", "Alps", "Central Europe"
]

# ReliefWeb RSS feed
RSS_FEED = (
    "https://reliefweb.int/feeds/updates?search=wildfire+OR+landslide&sl=environment-term_listing"
)

print("🌍 Fetching ReliefWeb RSS feed...")
response = requests.get(RSS_FEED)
soup = BeautifulSoup(response.content, features="xml")
items = soup.find_all("item")

inserted = 0
for item in items:
    title = item.title.text
    link = item.link.text
    description = item.description.text.strip()
    pub_date = item.pubDate.text.strip()
    published_at = datetime.strptime(pub_date, "%a, %d %b %Y %H:%M:%S %Z")
    # Skip if older than 14 days
    if published_at < datetime.utcnow().replace(tzinfo=timezone.utc) - timedelta(days=14):
        print(f"❌ Skipped (older than 14 days): {title}")
        continue

    text = f"{title} {description}".lower()

    # Detect disaster type
    disaster_type = None
    for dtype, keywords in DISASTER_KEYWORDS.items():
        if any(k.lower() in text for k in keywords):
            disaster_type = dtype
            break

    # Skip if no disaster keyword
    if not disaster_type:
        print(f"❌ Skipped (no disaster keyword): {title}")
        continue

    # Location match logic
    if not any(loc.lower() in text for loc in LOCATION_TERMS):
        print(f"⚠️ Matched disaster but no location: {title}")
        location_match = False
    else:
        location_match = True

    # Avoid duplicates and only insert if disaster_type is present
    if collection.count_documents({"url": link}) == 0 and disaster_type:
        print(f"✅ Matched: {title}")
        doc = {
            "title": title,
            "description": description,
            "url": link,
            "publishedAt": published_at,
            "source": "ReliefWeb",
            "source_type": "reliefweb_rss",
            "linked_disaster_type": disaster_type,
            "timestamp": datetime.utcnow(),
            "location_match": location_match
        }
        collection.insert_one(doc)
        inserted += 1

print(f"✅ Inserted {inserted} new articles from ReliefWeb RSS.")