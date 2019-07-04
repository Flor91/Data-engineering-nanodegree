# Instructions
# In this exercise, we’ll place our S3 to RedShift Copy operations into a SubDag.
# 1 - Consolidate HasRowsOperator into the SubDag
# 2 - Reorder the tasks to take advantage of the SubDag Operators

import datetime

from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.udacity_plugin import HasRowsOperator
from airflow.operators.udacity_plugin import S3ToRedshiftOperator

import sql_statements.py


# Returns a DAG which creates a table if it does not exist, and then proceeds
# to load data into that table from S3. When the load is complete, a data
# quality  check is performed to assert that at least one row of data is
# present.
def get_s3_to_redshift_dag(
        parent_dag_name,
        task_id,
        redshift_conn_id,
        aws_credentials_id,
        table,
        create_sql_stmt,
        s3_bucket,
        s3_key,
        *args, **kwargs):
    dag = DAG(
        f"{parent_dag_name}.{task_id}",
        **kwargs
    )

    create_task = PostgresOperator(
        task_id=f"create_{table}_table",
        dag=dag,
        postgres_conn_id=redshift_conn_id,
        sql=create_sql_stmt
    )

    copy_task = S3ToRedshiftOperator(
        task_id=f"load_{table}_from_s3_to_redshift",
        dag=dag,
        table=table,
        redshift_conn_id=redshift_conn_id,
        aws_credentials_id=aws_credentials_id,
        s3_bucket=s3_bucket,
        s3_key=s3_key
    )

    #
    # TODO: Move the HasRowsOperator task here from the DAG
    #

    create_task >> copy_task
    #
    # TODO: Use DAG ordering to place the check task
    #

    return dag
