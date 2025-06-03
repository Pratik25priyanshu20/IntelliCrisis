# data_ingestion/reddit_ingestor.py

import os
import praw
from datetime import datetime, timedelta
from pymongo import MongoClient
from dotenv import load_dotenv

# Load environment variables
load_dotenv("config/.env")
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
REDDIT_CLIENT_ID = os.getenv("REDDIT_CLIENT_ID")
REDDIT_CLIENT_SECRET = os.getenv("REDDIT_CLIENT_SECRET")
REDDIT_USER_AGENT = os.getenv("REDDIT_USER_AGENT", "disaster-news-analyzer")

# MongoDB setup
client = MongoClient(MONGO_URI)
db = client["disaster_db"]
collection = db["news_articles"]

# Reddit setup
reddit = praw.Reddit(
    client_id=REDDIT_CLIENT_ID,
    client_secret=REDDIT_CLIENT_SECRET,
    user_agent=REDDIT_USER_AGENT
)

# Configuration
KEYWORDS = [
    "Waldbrand", "Flächenbrand", "Vegetationsbrand", "Feuer", "Brand", "Löschflugzeug",
    "wildfire", "forest fire", "landslide", "Erdrutsch", "mudslide", "Bergsturz", "rockfall"
]
SUBREDDITS = ["germany", "de", "switzerland", "europe", "natureisfuckinglit", "news"]
POST_LIMIT = 100
DAYS_BACK = 10
cutoff_datetime = datetime.utcnow() - timedelta(days=DAYS_BACK)

inserted = 0
print("👾 Starting Reddit disaster news ingestion...")

for subreddit_name in SUBREDDITS:
    subreddit = reddit.subreddit(subreddit_name)
    for post in subreddit.new(limit=POST_LIMIT):
        created_utc = datetime.utcfromtimestamp(post.created_utc)
        if created_utc < cutoff_datetime:
            continue

        title = post.title
        description = post.selftext or ""
        url = post.url
        source = f"r/{subreddit_name}"

        if any(kw.lower() in title.lower() or kw.lower() in description.lower() for kw in KEYWORDS):
            if collection.count_documents({"url": url}) == 0:
                collection.insert_one({
                    "title": title,
                    "description": description,
                    "url": url,
                    "publishedAt": created_utc,
                    "source": source,
                    "source_type": "reddit",
                    "timestamp": datetime.utcnow()
                })
                inserted += 1

print(f"✅ Inserted {inserted} disaster articles from Reddit.")
