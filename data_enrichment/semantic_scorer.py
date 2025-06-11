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
THRESHOLD = 0.4  # Minimum similarity threshold to link article
OVERRIDE_EXISTING = os.getenv("OVERRIDE_EXISTING", "false").lower() == "true"

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

        # Extra priority match
        if any(keyword in text for keyword in ["Blatten", "Wallis", "Valais"]):
            update["priority_match"] = True

        doc_collection.update_one({"_id": article["_id"]}, {"$set": update})
        updated += 1

logger.info(f"✅ Scored and linked {updated} articles with semantic relevance >= {THRESHOLD}")

'''


# data_enrichment/semantic_scorer.py

import os
import logging
from datetime import datetime
from pymongo import MongoClient
from dotenv import load_dotenv
from sentence_transformers import SentenceTransformer, util

class SemanticScorer:
    def __init__(self, 
                 threshold=0.4, 
                 override_existing=False, 
                 event_collections=("landslides", "wildfires"),
                 mongo_db="disaster_db"):

        load_dotenv("config/.env")
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger(__name__)

        self.threshold = threshold
        self.override_existing = override_existing
        self.event_collections = event_collections
        self.mongo_client = MongoClient(os.getenv("MONGO_URI", "mongodb://localhost:27017/"))
        self.db = self.mongo_client[mongo_db]
        self.doc_collection = self.db["news_articles"]

        self.logger.info("🧠 Loading multilingual SBERT model...")
        self.model = SentenceTransformer("paraphrase-multilingual-mpnet-base-v2")

    def _load_events(self):
        all_events = []
        for collection_name in self.event_collections:
            events = list(self.db[collection_name].find({}))
            for event in events:
                event['disaster_type'] = collection_name
                all_events.append(event)
        return all_events

    def process_unscored_articles(self):
        self.logger.info("📦 Fetching disaster events...")
        events = self._load_events()
        self.logger.info(f"✅ Found {len(events)} disaster events from: {self.event_collections}")

        article_query = {} if self.override_existing else {"semantic_score": {"$exists": False}}
        articles = list(self.doc_collection.find(article_query))
        self.logger.info(f"📰 Loaded {len(articles)} news articles for scoring")

        if not events or not articles:
            self.logger.warning("⚠️ No events or articles found. Exiting.")
            return {"processed_count": 0}

        event_texts = [
            f"{e.get('disaster_type', '')} in {e.get('latitude', '')},{e.get('longitude', '')} on {e.get('acq_date', e.get('date', ''))}"
            for e in events
        ]
        event_embeddings = self.model.encode(event_texts, convert_to_tensor=True)

        updated = 0
        for article in articles:
            text = f"{article.get('title', '')} {article.get('description', '')}".strip()
            if not text:
                continue

            embedding = self.model.encode(text, convert_to_tensor=True)
            similarities = util.cos_sim(embedding, event_embeddings)[0]
            best_score = float(similarities.max())
            best_index = int(similarities.argmax())
            best_event = events[best_index]

            if best_score >= self.threshold:
                update = {
                    "semantic_score": round(best_score, 4),
                    "semantic_linked_disaster_id": best_event["_id"],
                    "linked_disaster_type": best_event.get("disaster_type", "unknown")
                }

                if any(k in text for k in ["Blatten", "Wallis", "Valais"]):
                    update["priority_match"] = True

                self.doc_collection.update_one({"_id": article["_id"]}, {"$set": update})
                updated += 1

        self.logger.info(f"✅ Scored and linked {updated} articles with semantic relevance >= {self.threshold}")
        return {"processed_count": updated}

if __name__ == "__main__":
    scorer = SemanticScorer()
    result = scorer.process_unscored_articles()
    print(result)