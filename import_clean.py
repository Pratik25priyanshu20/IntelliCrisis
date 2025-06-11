'''
from pymongo import MongoClient
from bson import json_util
import os

# Set your database path and connection
DATA_PATH = "/Users/futurediary/disaster-news-analyzer"
MONGO_URI = "mongodb://localhost:27017"
DB_NAME = "disaster_db"

# Connect to MongoDB
client = MongoClient("mongodb://intellicrisis-mongo:27017")
db = client[DB_NAME]

# Files and their collections
files = {
    "news_articles": "disaster_db.news_articles.json",
    "wildfires": "disaster_db.wildfires.json",
    "landslides": "disaster_db.landslides.json"
}

# Import loop
for collection_name, filename in files.items():
    print(f"📥 Importing {filename} → {collection_name} ...")
    file_path = os.path.join(DATA_PATH, filename)
    
    with open(file_path) as f:
        documents = json_util.loads(f.read())

    inserted = 0
    for doc in documents:
        doc.pop('_id', None)  # 🧹 Remove old _id to prevent duplication errors
        db[collection_name].insert_one(doc)
        inserted += 1

    print(f"✅ Inserted {inserted} documents into `{collection_name}`.\n")
    
    
    
    '''
from pymongo import MongoClient
import json

client = MongoClient("mongodb://localhost:27017")  # OK inside container
db = client["disaster_db"]

collections = {
    "news_articles": "/news.json",
    "wildfires": "/fires.json",
    "landslides": "/slides.json"
}

for name, path in collections.items():
    print(f"📥 Importing {path} → {name} ...")
    with open(path) as f:
        docs = json.load(f)

    inserted = 0
    for doc in docs:
        doc.pop("_id", None)
        db[name].insert_one(doc)
        inserted += 1

    print(f"✅ Inserted {inserted} documents into `{name}`.\n")