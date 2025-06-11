'''
import pandas as pd
import requests
from pymongo import MongoClient
from datetime import datetime
import os
from dotenv import load_dotenv
from io import StringIO

# Load config
load_dotenv(dotenv_path="config/.env")
NASA_KEY = os.getenv("NASA_MAP_KEY")
NASA_URL = os.getenv("NASA_FIRMS_URL")
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")

print("🔍 DEBUG: NASA_MAP_KEY =", os.getenv("NASA_MAP_KEY"))
print("🔍 DEBUG: NASA_FIRMS_URL =", os.getenv("NASA_FIRMS_URL"))

if not NASA_KEY or not NASA_URL:
    raise ValueError("❌ NASA_KEY or NASA_URL is missing. Check your .env file and ensure keys are correct.")

# Build request URL for Germany
DAYS = os.getenv("FIRMS_DAYS", "3")
endpoint = f"{NASA_URL}/{NASA_KEY}/VIIRS_SNPP_NRT/DEU/{DAYS}"
print(f"📡 Fetching from: {endpoint}")

# MongoDB setup
client = MongoClient(MONGO_URI)
db = client["disaster_db"]
collection = db["wildfires"]

# Fetch and read CSV
response = requests.get(endpoint)
if response.status_code != 200:
    print("❌ Failed to fetch data:", response.status_code)
    exit()

df = pd.read_csv(StringIO(response.text))
print(f"✅ Retrieved {len(df)} records")

# Normalize and insert
records = []
for _, row in df.iterrows():
    records.append({
        "country": row.get("country_id", "DEU"),
        "latitude": row.get("latitude"),
        "longitude": row.get("longitude"),
        "brightness": row.get("bright_ti4"),
        "confidence": row.get("confidence"),
        "acq_date": row.get("acq_date"),
        "acq_time": row.get("acq_time"),
        "satellite": row.get("satellite"),
        "instrument": row.get("instrument"),
        "frp": row.get("frp"),
        "daynight": row.get("daynight"),
        "source": "NASA_FIRMS",
        "timestamp": datetime.utcnow()
    })

# Insert to MongoDB
if records:
    collection.insert_many(records)
    print(f"🟢 Inserted {len(records)} wildfire records into MongoDB.")
else:
    print("⚠️ No records to insert.")
    
    
    
'''
import pandas as pd
import requests
from pymongo import MongoClient
from datetime import datetime, timedelta
import os
import logging
from dotenv import load_dotenv
from io import StringIO
import sys
from typing import List, Dict, Any

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('nasa_firms_ingestion.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def load_configuration() -> Dict[str, str]:
    """Load and validate configuration from environment variables."""
    load_dotenv(dotenv_path="config/.env")

    config = {
        'NASA_KEY': os.getenv("NASA_MAP_KEY"),
        'MONGO_URI': os.getenv("MONGO_URI", "mongodb://localhost:27017/"),
        'DB_NAME': os.getenv("DB_NAME", "intellicrisis"),
        'COLLECTION_NAME': os.getenv("COLLECTION_NAME", "wildfires"),
        'REQUEST_TIMEOUT': int(os.getenv("REQUEST_TIMEOUT", "30")),
        'DAYS_TO_FETCH': int(os.getenv("FIRMS_DAYS", "30"))  # Fetch 30 days by default
    }

    if not config['NASA_KEY']:
        raise ValueError("❌ NASA_KEY is missing. Check your .env file and ensure keys are correct.")

    logger.info("✅ Configuration loaded successfully")
    return config

def fetch_nasa_data(days: int, timeout: int, api_key: str) -> pd.DataFrame:
    """Fetch wildfire data in 10-day chunks up to 'days' total."""
    base_url = "https://firms.modaps.eosdis.nasa.gov/api/country/csv"
    country = "DEU"
    satellite = "VIIRS_SNPP_NRT"
    all_chunks = []

    for i in range(0, days, 10):
        chunk_days = min(10, days - i)
        endpoint = f"{base_url}/{api_key}/{satellite}/{country}/{chunk_days}"
        logger.info(f"📡 Fetching from: {endpoint}")

        try:
            response = requests.get(endpoint, timeout=timeout)
            response.raise_for_status()
            if not response.text.strip():
                logger.warning("⚠️ Empty response from NASA FIRMS")
                continue

            df_chunk = pd.read_csv(StringIO(response.text))
            logger.info(f"✅ Retrieved {len(df_chunk)} records from chunk")
            all_chunks.append(df_chunk)

        except Exception as e:
            logger.error(f"❌ Failed to fetch chunk: {e}")

    if all_chunks:
        full_df = pd.concat(all_chunks, ignore_index=True)
        return full_df
    else:
        return pd.DataFrame()

def validate_and_clean_data(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        logger.warning("⚠️ No data to validate")
        return df

    initial_count = len(df)
    required_fields = ['latitude', 'longitude', 'acq_date']
    df = df.dropna(subset=required_fields)
    df = df[
        (df['latitude'].between(-90, 90)) & 
        (df['longitude'].between(-180, 180))
    ].copy()

    if 'confidence' in df.columns:
        df['confidence'] = df['confidence'].astype(str).str.lower()
        df = df[df['confidence'].isin(['l', 'n', 'h'])]

    duplicate_fields = ['latitude', 'longitude', 'acq_date', 'acq_time']
    existing_fields = [field for field in duplicate_fields if field in df.columns]
    if existing_fields:
        df = df.drop_duplicates(subset=existing_fields)

    cleaned_count = len(df)
    removed_count = initial_count - cleaned_count

    if removed_count > 0:
        logger.info(f"🧹 Cleaned data: removed {removed_count} invalid/duplicate records")

    logger.info(f"✅ Data validation complete: {cleaned_count} valid records")
    return df

def transform_records(df: pd.DataFrame, country_code: str = "DEU") -> List[Dict[str, Any]]:
    if df.empty:
        return []

    records = []
    current_time = datetime.utcnow()

    for _, row in df.iterrows():
        confidence_map = {'l': 30, 'n': 60, 'h': 90}
        confidence_score = confidence_map.get(str(row.get("confidence")).lower(), None)

        record = {
            "country": row.get("country_id", country_code),
            "latitude": float(row.get("latitude")) if pd.notna(row.get("latitude")) else None,
            "longitude": float(row.get("longitude")) if pd.notna(row.get("longitude")) else None,
            "brightness": float(row.get("bright_ti4")) if pd.notna(row.get("bright_ti4")) else None,
            "confidence": confidence_score,
            "acq_date": row.get("acq_date"),
            "acq_time": row.get("acq_time"),
            "satellite": row.get("satellite"),
            "instrument": row.get("instrument"),
            "frp": float(row.get("frp")) if pd.notna(row.get("frp")) else None,
            "daynight": row.get("daynight"),
            "source": "NASA_FIRMS",
            "timestamp": current_time,
            "ingestion_date": current_time.strftime("%Y-%m-%d")
        }
        records.append(record)

    logger.info(f"🔄 Transformed {len(records)} records for MongoDB insertion")
    return records

def upsert_to_mongodb(records: List[Dict[str, Any]], mongo_uri: str, db_name: str, collection_name: str) -> None:
    if not records:
        logger.warning("⚠️ No records to insert into MongoDB")
        return

    try:
        with MongoClient(mongo_uri) as client:
            db = client[db_name]
            collection = db[collection_name]

            collection.create_index([
                ("latitude", 1),
                ("longitude", 1),
                ("acq_date", 1),
                ("acq_time", 1)
            ], background=True)

            inserted_count = 0
            updated_count = 0

            for record in records:
                filter_criteria = {
                    "latitude": record["latitude"],
                    "longitude": record["longitude"],
                    "acq_date": record["acq_date"],
                    "acq_time": record["acq_time"]
                }

                result = collection.update_one(
                    filter_criteria,
                    {"$set": record},
                    upsert=True
                )

                if result.upserted_id:
                    inserted_count += 1
                elif result.modified_count > 0:
                    updated_count += 1

            logger.info(f"🟢 MongoDB operation complete: {inserted_count} inserted, {updated_count} updated")

    except Exception as e:
        logger.error(f"❌ MongoDB operation failed: {e}")
        raise

def main():
    try:
        logger.info("🚀 Starting NASA FIRMS data ingestion")
        config = load_configuration()

        df = fetch_nasa_data(config['DAYS_TO_FETCH'], config['REQUEST_TIMEOUT'], config['NASA_KEY'])

        if df.empty:
            logger.info("ℹ️ No data to process. Exiting.")
            return

        df_clean = validate_and_clean_data(df)

        if df_clean.empty:
            logger.warning("⚠️ No valid data after cleaning. Exiting.")
            return

        records = transform_records(df_clean)

        upsert_to_mongodb(
            records,
            config['MONGO_URI'],
            config['DB_NAME'],
            config['COLLECTION_NAME']
        )

        logger.info("✅ NASA FIRMS data ingestion completed successfully")

    except Exception as e:
        logger.error(f"❌ Ingestion failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()