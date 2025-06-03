from pymongo import MongoClient
import pprint

client = MongoClient("mongodb://localhost:27017/")
db = client["disaster_db"]
news = db["news_articles"]

print("📦 Sample enriched articles with all key fields:\n")

for doc in news.find({
    "semantic_score": {"$exists": True},
    "relevance_score": {"$exists": True},
    "linked_fire_id": {"$exists": True},
    "match_score": {"$exists": True},
    "entities": {"$exists": True}
}).limit(3):
    pprint.pprint(doc)