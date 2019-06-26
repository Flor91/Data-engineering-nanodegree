# Instructions
# Use the Airflow context in the pythonoperator to complete the TODOs below. Once you are done, run your DAG and check the logs to see the context in use.

import datetime
import logging

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.S3_hook import S3Hook


# TODO: Extract ds, run_id, prev_ds, and next_ds from the kwargs, and log them
# NOTE: Look here for context variables passed in on kwargs:
#       https://airflow.apache.org/code.html#macros
def log_details(*args, **kwargs):
    logging.info(f"Execution date is {kwargs['ds']}")
    logging.info(f"My run id is {kwargs['run_id']}")
    previous_ds = kwargs.get('prev_ds')
    if previous_ds:
        logging.info(f"My previous run was on {previous_ds}")
    next_ds = kwargs.get('next_ds')
    if next_ds:
        logging.info(f"My next run will be {next_ds}")

dag = DAG(
    'lesson1.solution5',
    schedule_interval="@daily",
    start_date=datetime.datetime.now() - datetime.timedelta(days=2)
)

list_task = PythonOperator(
    task_id="log_details",
    python_callable=log_details,
    provide_context=True,
    dag=dag
)
