from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import sys
import os
import subprocess
import importlib

# Add project paths
sys.path.append("/opt/airflow/project_kafka")
sys.path.append("/opt/airflow/config")

default_args = {
    'owner': 'intellicrisis',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def run_newsapi_producer():
    try:
        print("\U0001F680 Running NewsAPI producer...")
        env = os.environ.copy()
        env['PYTHONPATH'] = '/opt/airflow/project_kafka:/opt/airflow/config:/opt/airflow'
        result = subprocess.run(
            ['python3', '/opt/airflow/project_kafka/producers/newsapi_producer.py'],
            capture_output=True,
            text=True,
            timeout=600,
            env=env,
            cwd='/opt/airflow'
        )
        if result.returncode == 0:
            print("✅ NewsAPI producer finished successfully")
            print("📋 Producer Output:")
            print(result.stdout)
        else:
            print(f"❌ Producer failed with return code: {result.returncode}")
            print(f"❌ Error output: {result.stderr}")
            print(f"📋 Standard output: {result.stdout}")
            raise Exception(f"NewsAPI producer failed with return code {result.returncode}")
    except subprocess.TimeoutExpired:
        print("❌ NewsAPI producer timed out after 10 minutes")
        raise Exception("NewsAPI producer timed out")
    except Exception as e:
        print(f"❌ Error in NewsAPI producer task: {str(e)}")
        raise

def check_kafka_connection():
    try:
        print("🔍 Checking Kafka connection...")
        test_script = '''
import sys
try:
    from kafka import KafkaProducer
    import json
    producer = KafkaProducer(
        bootstrap_servers="kafka:29092",
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        request_timeout_ms=10000,
        retries=3
    )
    metadata = producer.bootstrap_connected()
    if metadata:
        print("✅ Kafka connection successful")
    else:
        print("❌ Kafka connection failed")
        sys.exit(1)
    producer.close()
except Exception as e:
    print(f"❌ Kafka connection error: {str(e)}")
    sys.exit(1)
'''
        result = subprocess.run(
            ['python3', '-c', test_script],
            capture_output=True,
            text=True,
            timeout=30
        )
        if result.returncode == 0:
            print("✅ Kafka connectivity check passed")
            print(result.stdout)
        else:
            print(f"❌ Kafka connectivity check failed: {result.stderr}")
            raise Exception("Kafka is not accessible")
    except Exception as e:
        print(f"❌ Error checking Kafka connection: {str(e)}")
        raise

def verify_environment():
    try:
        print("🔍 Verifying environment setup...")

        env_file = "/opt/airflow/config/.env"
        if os.path.exists(env_file):
            print(f"✅ Environment file found: {env_file}")
        else:
            raise FileNotFoundError("❌ .env file is missing")

        producer_script = "/opt/airflow/project_kafka/producers/newsapi_producer.py"
        if os.path.exists(producer_script):
            print(f"✅ Producer script found: {producer_script}")
        else:
            raise FileNotFoundError("❌ Producer script is missing")

        packages = ['kafka', 'requests', 'dotenv']
        for pkg in packages:
            if importlib.util.find_spec(pkg) is None:
                raise ImportError(f"❌ Missing required package: {pkg}")
            print(f"✅ Package available: {pkg}")

        print("✅ Environment verification completed successfully")

    except Exception as e:
        print(f"❌ Environment verification failed: {str(e)}")
        raise

with DAG(
    dag_id='newsapi_ingestion_dag',
    default_args=default_args,
    description='Fetch disaster articles from NewsAPI and push to Kafka',
    schedule_interval='@daily',
    catchup=False,
    tags=['newsapi', 'kafka', 'intellicrisis'],
    max_active_runs=1,
) as dag:

    verify_env_task = PythonOperator(
        task_id='verify_environment',
        python_callable=verify_environment,
    )

    check_kafka_task = PythonOperator(
        task_id='check_kafka_connection',
        python_callable=check_kafka_connection,
    )

    run_producer_task = PythonOperator(
        task_id='run_newsapi_producer',
        python_callable=run_newsapi_producer,
    )

    verify_env_task >> check_kafka_task >> run_producer_task
