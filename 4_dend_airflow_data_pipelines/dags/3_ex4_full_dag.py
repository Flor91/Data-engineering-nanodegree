import datetime

from airflow import DAG

from airflow.operators import (
    FactsCalculatorOperator,
    HasRowsOperator,
    S3ToRedshiftOperator
)

#       This DAG performs the following functions
#       1. Loads Trip data from S3 to RedShift
#       2. Performs a data quality check on the Trips table in RedShift
#       3. Uses the FactsCalculatorOperator to create a Facts table in Redshift
#           a. **NOTE**: to complete this step you must complete the FactsCalcuatorOperator
#              skeleton defined in plugins/operators/facts_calculator.py
#
dag = DAG("lesson3.exercise4", start_date=datetime.datetime.utcnow())

copy_trips_task = S3ToRedshiftOperator(
    task_id="load_trips_from_s3_to_redshift",
    dag=dag,
    table="trips",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="data-pipelines/divvy/unpartitioned/divvy_trips_2018.csv"
)

check_trips = HasRowsOperator(
    task_id="trips_has_rows",
    dag=dag,
    redshift_conn_id="redshift",
    table="trips",
    provide_context=True
)

calculate_facts = FactsCalculatorOperator(
    task_id="calculate_facts",
    dag=dag,
    postgres_conn_id="redshift",
    origin_table="trips",
    destination_table="trips_facts",
    fact_column="tripduration",
    groupbycolumn="bikeid"
)

copy_trips_task >> check_trips
check_trips >> calculate_facts
