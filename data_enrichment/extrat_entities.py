import os
import spacy
from pymongo import MongoClient
from dotenv import load_dotenv

# Load env vars
load_dotenv(dotenv_path="config/.env")
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
client = MongoClient(MONGO_URI)

db = client["disaster_db"]
news_col = db["news_articles"]

# Load spaCy German model
print("🔍 Loading spaCy NER model (German)...")
nlp = spacy.load("de_core_news_sm")

updated = 0

# Only process articles with linked_fire_id and not yet processed
cursor = news_col.find({"linked_fire_id": {"$exists": True}, "entities": {"$exists": False}})

for article in cursor:
    text = f"{article.get('title', '')} {article.get('description', '')}".strip()
    doc = nlp(text)

    entities = []
    for ent in doc.ents:
        entities.append({
            "text": ent.text,
            "label": ent.label_
        })

    news_col.update_one(
        {"_id": article["_id"]},
        {"$set": {"entities": entities}}
    )
    updated += 1

print(f"✅ Extracted entities for {updated} articles.")