# airflow/dags/reddit_ingestion_dag.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import importlib.util
import os

default_args = {
    'owner': 'intellicrisis',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def run_reddit_producer():
    """Dynamically load and run reddit_producer.py"""
    try:
        producer_path = "/opt/airflow/project_kafka/producers/reddit_producer.py"
        
        if not os.path.exists(producer_path):
            raise FileNotFoundError(f"Producer file not found: {producer_path}")

        spec = importlib.util.spec_from_file_location("reddit_producer", producer_path)
        reddit_module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(reddit_module)

        print("✅ Reddit producer executed successfully")

    except Exception as e:
        print(f"❌ Reddit producer failed: {str(e)}")
        raise

dag = DAG(
    'reddit_ingestion_dag',
    default_args=default_args,
    description='Ingest Reddit disaster-related posts into Kafka',
    schedule_interval='@daily',
    catchup=False,
    tags=['reddit', 'kafka', 'ingestion']
)

reddit_task = PythonOperator(
    task_id='run_reddit_producer',
    python_callable=run_reddit_producer,
    dag=dag
)