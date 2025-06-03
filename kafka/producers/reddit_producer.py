import os
import praw
import json
from datetime import datetime, timedelta
from dotenv import load_dotenv
from kafka import KafkaProducer

# Load environment variables
load_dotenv("config/.env")
REDDIT_CLIENT_ID = os.getenv("REDDIT_CLIENT_ID")
REDDIT_CLIENT_SECRET = os.getenv("REDDIT_CLIENT_SECRET")
REDDIT_USER_AGENT = os.getenv("REDDIT_USER_AGENT", "disaster-news-analyzer")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC_REDDIT", "reddit_articles")

# Disaster keywords (EN & DE)
DISASTER_KEYWORDS = {
    "wildfire": [
        "Waldbrand", "Vegetationsbrand", "Flächenbrand", "Buschbrand", "Feuer", "Brand", "Löschflugzeug", "Flammen",
        "fire", "wildfire", "forest fire"
    ],
    "landslide": [
        "Erdrutsch", "Hangrutschung", "Bergsturz", "Schlammlawine", "Felssturz",
        "landslide", "mudslide", "rockfall", "debris flow"
    ]
}

# Subreddits to search
SUBREDDITS = ["germany", "de", "Switzerland", "natureisfuckinglit", "news"]
POST_LIMIT = 100
DAYS_WINDOW = 10

# Setup Reddit client
reddit = praw.Reddit(
    client_id=REDDIT_CLIENT_ID,
    client_secret=REDDIT_CLIENT_SECRET,
    user_agent=REDDIT_USER_AGENT
)

# Kafka producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(","),
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

print("👾 Starting Reddit disaster news ingestion...")
inserted = 0
since_timestamp = datetime.utcnow() - timedelta(days=DAYS_WINDOW)

for subreddit_name in SUBREDDITS:
    subreddit = reddit.subreddit(subreddit_name)
    for post in subreddit.new(limit=POST_LIMIT):
        post_time = datetime.utcfromtimestamp(post.created_utc)
        if post_time < since_timestamp:
            continue

        title = post.title
        description = post.selftext or ""
        url = post.url

        text_combined = f"{title} {description}".lower()

        for dtype, keywords in DISASTER_KEYWORDS.items():
            if any(kw.lower() in text_combined for kw in keywords):
                record = {
                    "title": title,
                    "description": description,
                    "url": url,
                    "publishedAt": post_time.isoformat(),
                    "source": f"r/{subreddit_name}",
                    "source_type": "reddit",
                    "disaster_type": dtype,
                    "timestamp": datetime.utcnow().isoformat()
                }
                producer.send(KAFKA_TOPIC, value=record)
                print(f"📤 Sent to Kafka: [{dtype}] {title[:60]}...")
                inserted += 1
                break  # Don't match multiple types per post

print(f"✅ Inserted {inserted} Reddit articles to Kafka.")
producer.flush()