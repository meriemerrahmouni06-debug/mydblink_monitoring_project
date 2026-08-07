from airflow import DAG #pipeline orchestree
from airflow.operators.python import PythonOperator #airflow aperator qui execute une fonction python
from datetime import datetime, timedelta
import subprocess #to run other programms (externes)
import logging #to write messages (Airflow logs) 

default_args = {
    'owner': 'meriem',
    'retries': 2,#try before exit
    'retry_delay': timedelta(minutes=2),# 2 minutes between retries.
}

def run_producer():
    result = subprocess.run(
        ["python", "/opt/airflow/producer/producer.py"],
        check=True,
        capture_output=True,
        text=True,
    )
    logging.info(result.stdout) #affiche les sorties dans les logs airflow
    if result.stderr:
        logging.warning(result.stderr)#sil y a une erreur s'affiche dans les logs

def run_consumer():
    result = subprocess.run(
        ["python", "/opt/airflow/consumer/consumer.py"],#runner a partir d'airflow
        check=True,
        capture_output=True,
        text=True,
    )
    logging.info(result.stdout)
    if result.stderr:
        logging.warning(result.stderr)

with DAG(
    dag_id='monitoring_pipeline',#nom unique de ce DAG
    default_args=default_args,#
    schedule_interval='@daily',   # a ajuster par table plus tard si besoin
    start_date=datetime(2026, 8, 7),
    catchup=False,#ignore le passé, ne traite que les cycles a partir de maintenant
    tags=['monitoring', 'kafka'],
) as dag:
    #2 tasks 
    task_producer = PythonOperator(
        task_id='run_producer',
        python_callable=run_producer,#fct python
    )

    task_consumer = PythonOperator(
        task_id='run_consumer',
        python_callable=run_consumer,#fct python.
    )
    #order producer -> consumer 
    task_producer >> task_consumer