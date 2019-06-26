# Define a function that uses the python logger to log a function. 
# Then finish filling in the details of the DAG down below. 
# Once you’ve done that, run "/opt/airflow/start.sh" command to start the web server. 
# Once the Airflow web server is ready,  open the Airflow UI using the "Access Airflow" button. 
# Turn your DAG “On”, and then Run your DAG. 

import datetime
import logging

from airflow import DAG
from airflow.operators.python_operator import PythonOperator


def hello_world():
    logging.info("Hello Flor!")


dag = DAG(
        'lesson1.solution1',
        start_date=datetime.datetime.now())

greet_task = PythonOperator(
    task_id="hello_world_task",
    python_callable=hello_world,
    dag=dag
)
