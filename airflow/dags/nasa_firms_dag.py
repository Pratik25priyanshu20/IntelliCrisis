from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import subprocess
import sys
import os

default_args = {
    'owner': 'intellicrisis',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=10)
}

def run_nasa_firms_producer():
    try:
        print("🚀 Running NASA FIRMS producer...")

        # Set environment with proper PYTHONPATH
        env = os.environ.copy()
        env['PYTHONPATH'] = '/opt/airflow/project_kafka:/opt/airflow/config:/opt/airflow'

        result = subprocess.run(
            [sys.executable, "/opt/airflow/project_kafka/producers/nasa_firms_producer.py"],
            capture_output=True,
            text=True,
            timeout=300,
            env=env,
            cwd='/opt/airflow'
        )

        if result.returncode != 0:
            print(f"❌ Producer failed with return code {result.returncode}")
            print(result.stderr)
            raise Exception(result.stderr)

        print("✅ NASA FIRMS producer output:")
        print(result.stdout)

    except Exception as e:
        print(f"❌ Error running NASA FIRMS producer: {str(e)}")
        raise

dag = DAG(
    'nasa_firms_producer_dag',
    default_args=default_args,
    description='Fetch wildfire data from NASA FIRMS and send to Kafka',
    schedule_interval=timedelta(hours=6),
    catchup=False,
    tags=['intellicrisis', 'nasa', 'wildfire']
)

run_task = PythonOperator(
    task_id='run_nasa_firms_producer',
    python_callable=run_nasa_firms_producer,
    dag=dag
)