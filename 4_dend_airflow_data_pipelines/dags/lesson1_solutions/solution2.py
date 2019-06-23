import datetime
import logging

from airflow import DAG
from airflow.operators.python_operator import PythonOperator


def hello_world():
    logging.info("Hello World")


dag = DAG(
        "lesson1.solution2",
        start_date=datetime.datetime.now() - datetime.timedelta(days=2),
        schedule_interval='@daily')

task = PythonOperator(
        task_id="hello_world_task",
        python_callable=hello_world,
        dag=dag)
