from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from etl_pipeline import run_etl

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=5),
}

with DAG(
    'api_to_postgres',
    default_args=default_args,
    description='Fetch API data, transform, and store in PostgreSQL',
    schedule_interval=timedelta(seconds=10),  # Run every 10 seconds
    start_date=datetime(2024, 11, 21),
    catchup=False,
) as dag:

    fetch_transform_load = PythonOperator(
        task_id='fetch_transform_load',
        python_callable=run_etl,
    )

    fetch_transform_load
