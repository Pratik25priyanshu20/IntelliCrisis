import os
from pymongo import MongoClient
from dotenv import load_dotenv
from datetime import datetime, timedelta
from geopy.distance import geodesic

# Load env
load_dotenv(dotenv_path="config/.env")
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
client = MongoClient(MONGO_URI)

db = client["disaster_db"]
news_col = db["news_articles"]
fire_col = db["wildfires"]

# Set thresholds
SEMANTIC_THRESHOLD = 0.2
TIME_WINDOW_DAYS = 2
DISTANCE_THRESHOLD_KM = 50

linked = 0

# Fetch fires
fire_events = list(fire_col.find())

for article in news_col.find({"semantic_score": {"$gte": SEMANTIC_THRESHOLD}}):
    article_time = datetime.strptime(article["publishedAt"], "%Y-%m-%dT%H:%M:%SZ")
    article_coords = (article.get("query_lat"), article.get("query_lon"))

    best_match = None
    best_score = float("inf")  # lower is better

    for fire in fire_events:
        fire_time = datetime.strptime(fire["acq_date"], "%Y-%m-%d")
        time_diff = abs((article_time - fire_time).days)

        if time_diff > TIME_WINDOW_DAYS:
            continue

        fire_coords = (fire["latitude"], fire["longitude"])
        dist_km = geodesic(article_coords, fire_coords).km

        if dist_km > DISTANCE_THRESHOLD_KM:
            continue

        score = dist_km + time_diff * 10  # weight time more

        if score < best_score:
            best_score = score
            best_match = fire["_id"]

    if best_match:
        news_col.update_one(
            {"_id": article["_id"]},
            {"$set": {"linked_fire_id": best_match, "match_score": round(best_score, 2)}}
        )
        linked += 1

print(f"🔗 Linked {linked} articles to fire events.")