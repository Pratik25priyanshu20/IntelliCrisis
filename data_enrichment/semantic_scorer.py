# data_enrichment/semantic_scorer.py
'''
import os
from dotenv import load_dotenv
from pymongo import MongoClient
from sentence_transformers import SentenceTransformer, util

# Load environment
load_dotenv("config/.env")
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")

# Connect to MongoDB
client = MongoClient(MONGO_URI)
db = client["disaster_db"]
fires = list(db["wildfires"].find())
articles = db["news_articles"]

# Load multilingual semantic model
print("🧠 Loading multilingual BERT...")
model = SentenceTransformer("distiluse-base-multilingual-cased")

# Create fire embeddings
fire_texts = [
    f"wildfire in {fire.get('latitude')},{fire.get('longitude')} on {fire.get('acq_date')}"
    for fire in fires
]
fire_embeddings = model.encode(fire_texts, convert_to_tensor=True)

# Score each article
print("🔎 Scoring articles...")
updated = 0
cursor = articles.find({
    "$or": [
        {"semantic_score": {"$exists": False}},
        {"semantic_score": None}
    ]
})

for article in cursor:
    full_text = f"{article.get('title', '')} {article.get('description', '')}".strip()
    if not full_text:
        continue

    article_embedding = model.encode(full_text, convert_to_tensor=True)
    similarity_scores = util.cos_sim(article_embedding, fire_embeddings)[0]
    best_score = float(similarity_scores.max())
    best_index = int(similarity_scores.argmax())
    best_fire = fires[best_index]

    articles.update_one(
        {"_id": article["_id"]},
        {
            "$set": {
                "semantic_score": round(best_score, 4),
                "semantic_linked_fire_id": best_fire["_id"]
            }
        }
    )
    updated += 1

print(f"✅ Scored {updated} articles with semantic similarity.")
'''


import os
import logging
from datetime import datetime
from pymongo import MongoClient
from dotenv import load_dotenv
from sentence_transformers import SentenceTransformer, util

# Setup
load_dotenv("config/.env")
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# MongoDB Connection
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
client = MongoClient(MONGO_URI)
db = client["disaster_db"]
events_collection = db["landslides"]  # Can be switched to wildfires or merged
doc_collection = db["news_articles"]

# Parameters
THRESHOLD = 0.4  # Update this to control minimum match confidence
OVERRIDE_EXISTING = True  # Set to False if you only want to score unscored ones

# Load model
logger.info("🧠 Loading multilingual SBERT model...")
model = SentenceTransformer("paraphrase-multilingual-mpnet-base-v2")

# Load disaster events
logger.info("📦 Fetching disaster events...")
events = list(events_collection.find({}))
logger.info(f"✅ Found {len(events)} disaster events")

# Load articles
article_query = {} if OVERRIDE_EXISTING else {"semantic_score": {"$exists": False}}
articles = list(doc_collection.find(article_query))
logger.info(f"📰 Loaded {len(articles)} news articles for scoring")

if not events or not articles:
    logger.warning("⚠️ No events or articles found. Exiting.")
    exit()

# Create event embeddings
event_texts = [
    f"{e.get('disaster_type', '')} in {e.get('latitude', '')},{e.get('longitude', '')} on {e.get('acq_date', e.get('date', ''))}"
    for e in events
]
event_embeddings = model.encode(event_texts, convert_to_tensor=True)

# Score loop
updated = 0
for article in articles:
    text = f"{article.get('title', '')} {article.get('description', '')}".strip()
    if not text:
        continue

    embedding = model.encode(text, convert_to_tensor=True)
    similarities = util.cos_sim(embedding, event_embeddings)[0]
    best_score = float(similarities.max())
    best_index = int(similarities.argmax())
    best_event = events[best_index]

    # Only update if above threshold
    if best_score >= THRESHOLD:
        update = {
            "semantic_score": round(best_score, 4),
            "semantic_linked_disaster_id": best_event["_id"],
            "linked_disaster_type": best_event.get("disaster_type", "landslide")
        }
        
        if any(keyword in text for keyword in ["Blatten", "Wallis", "Valais"]):
            update["priority_match"] = True

        doc_collection.update_one({"_id": article["_id"]}, {"$set": update})
        updated += 1

logger.info(f"✅ Scored and linked {updated} articles with semantic relevance >= {THRESHOLD}")