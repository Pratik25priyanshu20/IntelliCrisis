from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import sys

# ✅ Fix path to reach Kafka producers
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../kafka/producers")))
from gnews_producer import run as run_gnews  # ✅ Import the producer function

def ingest_gnews():
    run_gnews()

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    dag_id='gnews_ingestion_dag',
    default_args=default_args,
    description='Ingest GNews articles and push to Kafka',
    schedule_interval='@hourly',
    start_date=datetime(2024, 1, 1),
    catchup=False
)

task = PythonOperator(
    task_id='run_gnews_producer',
    python_callable=ingest_gnews,
    dag=dag,
)