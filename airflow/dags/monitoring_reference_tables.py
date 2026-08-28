from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import subprocess
import logging

default_args = {
    'owner': 'meriem',
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
}

def run_producer():
    try:
        result = subprocess.run(
            ["python", "/opt/airflow/producer/producer.py"],
            check=True,
            capture_output=True,
            text=True,
        )
        logging.info(result.stdout)
    except subprocess.CalledProcessError as e:
        logging.error(f"STDOUT: {e.stdout}")
        logging.error(f"STDERR: {e.stderr}")
        raise


def run_consumer():
    try:
        result = subprocess.run(
            ["python", "/opt/airflow/consumer/consumer.py"],
            check=True,
            capture_output=True,
            text=True,
        )
        logging.info(result.stdout)
    except subprocess.CalledProcessError as e:
        logging.error(f"STDOUT: {e.stdout}")
        logging.error(f"STDERR: {e.stderr}")
        raise


def run_dbt():
    """Exécute dbt run directement dans le container Airflow (dbt-postgres installé via pip)."""
    try:
        result = subprocess.run(
    [
        "dbt", "run",
        "--project-dir", "/opt/airflow/monitoring_dbt",
        "--profiles-dir", "/opt/airflow/monitoring_dbt",
        "--log-path", "/tmp/dbt_logs",
        "--target-path", "/tmp/dbt_target",
    ],
    check=True,
    capture_output=True,
    text=True,
)
        logging.info(result.stdout)
        if result.stderr:
            logging.warning(result.stderr)
    except subprocess.CalledProcessError as e:
        logging.error(f"STDOUT: {e.stdout}")
        logging.error(f"STDERR: {e.stderr}")
        raise


with DAG(
    dag_id='monitoring_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    start_date=datetime(2026, 8, 7),
    catchup=False,
    tags=['monitoring', 'kafka', 'dbt'],
) as dag:

    task_producer = PythonOperator(
        task_id='run_producer',
        python_callable=run_producer,
    )

    task_consumer = PythonOperator(
        task_id='run_consumer',
        python_callable=run_consumer,
    )

    task_dbt = PythonOperator(
        task_id='run_dbt',
        python_callable=run_dbt,
    )

    # producer -> consumer -> dbt
    task_producer >> task_consumer >> task_dbt