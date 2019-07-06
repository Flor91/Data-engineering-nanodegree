import datetime
import logging

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.S3_hook import S3Hook

#
#  We're going to create a connection and a variable.
# 1. Open your browser to localhost:8080 and open Admin->Variables
# 2. Click "Create"
# 3. Set "Key" equal to "s3_bucket" and set "Val" equal to "udacity-dend"
# 4. Set "Key" equal to "s3_prefix" and set "Val" equal to "data-pipelines"
# 5. Click save
# 6. Open Admin->Connections
# 7. Click "Create"
# 8. Set "Conn Id" to "aws_credentials", "Conn Type" to "Amazon Web Services"
# 9. Set "Login" to your aws_access_key_id and "Password" to your aws_secret_key
# 10. Click save
# 11. Run the DAG

def list_keys():
    hook = S3Hook(aws_conn_id='aws_credentials')
    bucket = Variable.get('s3_bucket')
    prefix = Variable.get('s3_prefix')
    logging.info(f"Listing Keys from {bucket}/{prefix}")
    keys = hook.list_keys(bucket, prefix=prefix)
    for key in keys:
        logging.info(f"- s3://{bucket}/{key}")


dag = DAG(
        'lesson1.exercise4',
        start_date=datetime.datetime.now())

list_task = PythonOperator(
    task_id="list_keys",
    python_callable=list_keys,
    dag=dag
)
