import importlib.util
import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

# Load your main logic from project root
spec = importlib.util.spec_from_file_location(
    "crypto_warehouse",
    "/opt/airflow/project/crypto_warehouse.py"
)
crypto_module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(crypto_module)

with DAG(
    dag_id="crypto_pipeline",
    start_date=pendulum.datetime(2026, 10, 1, tz="Africa/Lagos"),
    schedule="0 8 * * *",  # now this is 8am Lagos = 7am UTC
    catchup=False,  # Don't backfill old days
    tags=["crypto", "etl"],
    description="Daily crypto warehouse ETL",
    max_active_runs=1,
) as dag:

    run_crypto_pipeline = PythonOperator(
        task_id="run_crypto_pipeline",
        python_callable=crypto_module.run_pipeline,
    )

    run_crypto_pipeline
