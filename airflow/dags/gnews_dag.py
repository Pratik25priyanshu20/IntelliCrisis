from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import sys
import importlib.util

def ingest_gnews():
    """Function to execute GNews producer using dynamic import"""
    try:
        # Dynamic import to avoid path issues
        producer_path = "/opt/airflow/project_kafka/producers/gnews_producer.py"
        
        if not os.path.exists(producer_path):
            raise FileNotFoundError(f"Producer file not found at: {producer_path}")
        
        # Load the module dynamically
        spec = importlib.util.spec_from_file_location("gnews_producer", producer_path)
        gnews_module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(gnews_module)
        
        # Execute the run function
        if hasattr(gnews_module, 'run'):
            gnews_module.run()
            print("✅ GNews ingestion completed successfully")
        else:
            raise AttributeError("'run' function not found in gnews_producer module")
            
    except Exception as e:
        print(f"❌ Error in GNews ingestion: {str(e)}")
        print(f"Current working directory: {os.getcwd()}")
        print(f"Python path: {sys.path}")
        raise

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 1, 1),
}

dag = DAG(
    dag_id='gnews_ingestion_dag',
    default_args=default_args,
    description='Ingest GNews articles and push to Kafka',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['news', 'kafka', 'ingestion']
)

gnews_task = PythonOperator(
    task_id='run_gnews_producer',
    python_callable=ingest_gnews,
    dag=dag,
)