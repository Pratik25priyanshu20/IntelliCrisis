from airflow import DAG
from airflow.operators.dummy import DummyOperator
from datetime import datetime

dag = DAG(
    dag_id="test_dummy_dag",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@once",
    catchup=False
)

start = DummyOperator(task_id="start", dag=dag)