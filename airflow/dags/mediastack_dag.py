# airflow/dags/mediastack_dag.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import sys
import importlib.util

def ingest_mediastack():
    """Function to execute MediaStack producer using dynamic import"""
    try:
        # Dynamic import to avoid path issues
        producer_path = "/opt/airflow/project_kafka/producers/mediastack_producer.py"
        
        if not os.path.exists(producer_path):
            raise FileNotFoundError(f"Producer file not found at: {producer_path}")
        
        # Load the module dynamically
        spec = importlib.util.spec_from_file_location("mediastack_producer", producer_path)
        mediastack_module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mediastack_module)
        
        # Execute the run function
        if hasattr(mediastack_module, 'run'):
            mediastack_module.run()
            print("✅ MediaStack ingestion completed successfully")
        else:
            raise AttributeError("'run' function not found in mediastack_producer module")
            
    except Exception as e:
        print(f"❌ Error in MediaStack ingestion: {str(e)}")
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
    dag_id='mediastack_ingestion_dag',
    default_args=default_args,
    description='Ingest disaster articles from MediaStack and push to Kafka',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['news', 'kafka', 'ingestion', 'mediastack']
)

mediastack_task = PythonOperator(
    task_id='run_mediastack_producer',
    python_callable=ingest_mediastack,
    dag=dag,
)