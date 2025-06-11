# semantic_scorer_dag.py
'''
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
    
    '''
    

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import sys
import os

# Add project paths to sys.path for context (optional with dynamic import)
sys.path.append('/opt/airflow/data_enrichment')
sys.path.append('/opt/airflow/config')

default_args = {
    'owner': 'intellicrisis',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

def run_semantic_scoring():
    """
    Dynamically import and execute semantic scoring logic
    """
    try:
        import importlib.util

        scorer_path = "/opt/airflow/data_enrichment/semantic_scorer.py"
        if not os.path.exists(scorer_path):
            raise FileNotFoundError(f"semantic_scorer.py not found at: {scorer_path}")

        spec = importlib.util.spec_from_file_location("semantic_scorer", scorer_path)
        scorer_module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(scorer_module)

        scorer = scorer_module.SemanticScorer(override_existing=False)  # Only score new data
        results = scorer.process_unscored_articles()

        print(f"✅ Semantic scoring completed. Processed {results.get('processed_count', 0)} articles")
        return results

    except Exception as e:
        print(f"❌ Error in semantic scoring: {str(e)}")
        raise

# DAG definition
dag = DAG(
    'semantic_scorer_dag',
    default_args=default_args,
    description='Semantic scoring and linking of news articles to disaster events',
    schedule_interval=timedelta(hours=6),  # Run every 6 hours
    catchup=False,
    tags=['semantic', 'intellicrisis', 'scoring']
)

# PythonOperator task
semantic_scoring_task = PythonOperator(
    task_id='run_semantic_scorer',
    python_callable=run_semantic_scoring,
    dag=dag,
)

# Optional: Health check or log task
log_completion = BashOperator(
    task_id='log_scoring_completion',
    bash_command='echo "✅ Semantic scoring DAG finished at $(date)"',
    dag=dag,
)

# DAG flow
semantic_scoring_task >> log_completion