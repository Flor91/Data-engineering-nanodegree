from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.subdag_operator import SubDagOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                DataQualityOperator, CreateTablesOperator)
from helpers import SqlQueries
from sparkify_dend_dimesions_subdag import load_dimensional_tables_dag


start_date = datetime.utcnow()

default_args = {
    'owner': 'florencia',
    'start_date': datetime(2018, 5, 1),
    'end_date': datetime(2018, 11, 30),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False
}

dag_name='sparkify_dend_dag'
dag = DAG(dag_name,
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *',
          max_active_runs=3
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

create_redshift_tables = CreateTablesOperator(
    task_id='Create_tables',
    dag=dag,
    redshift_conn_id="redshift"
)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    provide_context=True,
    table="events",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="log_data",
    region="us-west-2",
    file_format="JSON",
    execution_date=start_date
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    provide_context=True,
    table="songs",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="song_data",
    region="us-west-2",
    data_format="JSON",
    execution_date=start_date
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    provide_context=True,
    aws_credentials_id="aws_credentials",
    redshift_conn_id='redshift',
    sql_query=SqlQueries.songplay_table_insert
)

load_user_dimension_table_task_id='Load_user_dim_table'
load_user_dimension_table = SubDagOperator(
    subdag=load_dimensional_tables_dag(
        parent_dag_name=dag_name,
        task_id=load_user_dimension_table_task_id,
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        start_date= datetime(2018, 5, 1),
        table="users",
        sql_query=SqlQueries.user_table_insert,
    ),
    task_id=load_user_dimension_table_task_id,
    dag=dag,
)

load_song_dimension_table_task_id='Load_song_dim_table'
load_song_dimension_table = SubDagOperator(
    subdag=load_dimensional_tables_dag(
        parent_dag_name=dag_name,
        task_id=load_song_dimension_table_task_id,
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        start_date= datetime(2018, 5, 1),
        table="users",
        sql_query=SqlQueries.song_table_insert,
    ),
    task_id=load_song_dimension_table_task_id,
    dag=dag,
)

load_artist_dimension_table_task_id='Load_artist_dim_table'
load_artist_dimension_table = SubDagOperator(
    subdag=load_dimensional_tables_dag(
        parent_dag_name=dag_name,
        task_id=load_artist_dimension_table_task_id,
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        table="users",
        start_date= datetime(2018, 5, 1),
        sql_query=SqlQueries.artist_table_insert,
    ),
    task_id=load_artist_dimension_table_task_id,
    dag=dag,
)

load_time_dimension_table_task_id='Load_artist_dim_table'
load_time_dimension_table = SubDagOperator(
    subdag=load_dimensional_tables_dag(
        parent_dag_name=dag_name,
        task_id=load_artist_dimension_table_task_id,
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        table="users",
        start_date= datetime(2018, 5, 1),
        sql_query=SqlQueries.artist_table_insert,
    ),
    task_id=load_artist_dimension_table_task_id,
    dag=dag,
)


run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    provide_context=True,
    aws_credentials_id="aws_credentials",
    redshift_conn_id='redshift',
    tables=["songplay", "users", "song", "artist", "time"]
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

# Setting tasks dependencies

start_operator >> create_redshift_tables >> [stage_songs_to_redshift, stage_events_to_redshift]

[stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table

load_songplays_table >> [load_user_dimension_table, load_song_dimension_table, load_artist_dimension_table,
                           load_time_dimension_table] >> run_quality_checks

run_quality_checks >> end_operator

