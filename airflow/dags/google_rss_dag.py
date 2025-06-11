# airflow/dags/google_rss_dag.py

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import subprocess
import sys

default_args = {
    'owner': 'intellicrisis',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=10),
}

def run_google_rss_producer():
    try:
        result = subprocess.run(
            [sys.executable, "/opt/airflow/project_kafka/producers/google_rss_producer.py"],
            capture_output=True,
            text=True,
            timeout=300
        )
        if result.returncode != 0:
            raise Exception(f"Producer failed:\n{result.stderr}")
        print(result.stdout)
    except Exception as e:
        print(f"❌ Google RSS Producer Error: {e}")
        raise

dag = DAG(
    'google_rss_ingestion_dag',
    default_args=default_args,
    description='Ingest disaster-related news via Google RSS and push to Kafka',
    schedule_interval=timedelta(hours=6),
    catchup=False,
    tags=['intellicrisis', 'google_rss', 'producer'],
)

start_task = BashOperator(
    task_id='log_start',
    bash_command='echo "🚀 Starting Google RSS Producer DAG at $(date)"',
    dag=dag
)

producer_task = PythonOperator(
    task_id='run_google_rss_producer',
    python_callable=run_google_rss_producer,
    dag=dag
)

finish_task = BashOperator(
    task_id='log_completion',
    bash_command='echo "✅ Completed Google RSS Producer DAG at $(date)"',
    dag=dag
)

start_task >> producer_task >> finish_task