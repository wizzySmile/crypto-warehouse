from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime


def run_crypto_pipeline():
    from crypto_warehouse import run_pipeline
    run_pipeline()

with DAG(
    dag_id="crypto_pipeline",
    start_date=datetime(2026, 10, 25),
    schedule= "@daily",
    catchup=False
) as dag:
    pipeline_task = PythonOperator(
        task_id="run_crypto_pipeline",
        python_callable=run_crypto_pipeline
    )
