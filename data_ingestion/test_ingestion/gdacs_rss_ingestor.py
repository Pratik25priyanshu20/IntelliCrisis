# data_ingestion/gdacs_rss_ingestor.py

import requests
from bs4 import BeautifulSoup
from pymongo import MongoClient
from datetime import datetime
from email.utils import parsedate_to_datetime

# MongoDB setup
client = MongoClient("mongodb://localhost:27017/")
db = client["disaster_db"]
disaster_collection = db["disaster_events"]

# GDACS RSS Feed URL
rss_url = "https://www.gdacs.org/rss.aspx?profile=DISASTER"

print("🌍 Fetching GDACS RSS feed for global disasters...")
response = requests.get(rss_url)
soup = BeautifulSoup(response.content, features="xml")
items = soup.find_all("item")

inserted = 0
for item in items:
    title = item.title.text if item.title else ""
    if "landslide" not in title.lower():
        continue

    description = item.description.text if item.description else ""
    pub_date = parsedate_to_datetime(item.pubDate.text.strip()) if item.pubDate else datetime.utcnow()
    link = item.link.text if item.link else ""
    guid = item.guid.text.strip() if item.guid else link

    if disaster_collection.count_documents({"guid": guid}) == 0:
        disaster_collection.insert_one({
            "type": "landslide",
            "title": title,
            "description": description,
            "link": link,
            "guid": guid,
            "publishedAt": pub_date,
            "source": "GDACS RSS",
            "timestamp": datetime.utcnow()
        })
        inserted += 1

print(f"✅ Inserted {inserted} new landslide disaster events from GDACS.")