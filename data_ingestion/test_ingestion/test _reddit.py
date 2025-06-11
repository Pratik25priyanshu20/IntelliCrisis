import os
from dotenv import load_dotenv
from pathlib import Path
import praw

# Dynamically find the .env file in project_root/config/.env
env_path = Path(__file__).resolve().parents[2] / "config" / ".env"
load_dotenv(dotenv_path=env_path)

# DEBUG: Check if variables are loaded
print("🔍 Client ID:", os.getenv("REDDIT_CLIENT_ID"))
print("🔍 Client Secret:", os.getenv("REDDIT_CLIENT_SECRET"))
print("🔍 User Agent:", os.getenv("REDDIT_USER_AGENT"))

# Setup Reddit client
reddit = praw.Reddit(
    client_id=os.getenv("REDDIT_CLIENT_ID"),
    client_secret=os.getenv("REDDIT_CLIENT_SECRET"),
    user_agent=os.getenv("REDDIT_USER_AGENT")
)

# Test query
for submission in reddit.subreddit("news").search("wildfire", limit=5):
    print(f"🔥 {submission.title} ({submission.score}) - {submission.url}")