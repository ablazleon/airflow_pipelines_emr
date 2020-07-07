from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')
s3_bucket = 'udacity-dend'
song_s3_key = "song_data"
log_s3_key = "log-data"
log_file = "log_json_path.json"

# Srat and end dates are set not to run repeteadly
default_args = {
    'owner': 'Raul',
    'start_date': datetime(2019, 1, 12),
    'end_date': datetime(2019, 12, 1),
    'depends_on_past': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id = 'Stage_events',
    table = "staging_events",
    s3_bucket = s3_bucket,
    s3_key = log_s3_key,
    file_format = "JSON",
    log_file = log_file,
    redshift_conn_id = "redshift",
    aws_conn_id = "aws_credential",
    dag = dag,
    provide_context = True
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id = 'Stage_songs',
    dag = dag,
    provide_context = True,
    aws_conn_id = "aws_credentials",
    redshift_conn_id = "redshift",
    s3_bucket = s3_bucket,
    s3_key = "song_data",
    table = "staging_songs",
    statement = SqlQueries.create_table_staging_songs
)

load_songplays_table = LoadFactOperator(
    task_id = 'Load_songplays_fact_table',
    dag = dag,
    provide_context = True,
    aws_conn_id = "aws_credentials",
    redshift_conn_id = 'redshift',
    statement = SqlQueries.create_table_songplays,
    query = SqlQueries.songplay_table_insert
)

load_user_dimension_table = LoadDimensionOperator(
    task_id = 'Load_user_dim_table',
    dag = dag,
    provide_context = True,
    aws_conn_id = "aws_credentials",
    redshift_conn_id = 'redshift',
    statement = SqlQueries.create_table_users,
    query = SqlQueries.user_table_insert
)

load_song_dimension_table = LoadDimensionOperator(
    task_id = 'Load_song_dim_table',
    dag = dag,
    provide_context = True,
    aws_conn_id = "aws_credentials",
    redshift_conn_id = 'redshift',
    statement = SqlQueries.create_table_songs,
    query = SqlQueries.song_table_insert
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id = 'Load_artist_dim_table',
    dag = dag,
    provide_context = True,
    aws_conn_id = "aws_credentials",
    redshift_conn_id = 'redshift',
    statement = SqlQueries.create_table_artist,
    query = SqlQueries.artist_table_insert
)

load_time_dimension_table = LoadDimensionOperator(
    task_id = 'Load_time_dim_table',
    dag = dag,
    provide_context = True,
    aws_conn_id = "aws_credentials",
    redshift_conn_id = 'redshift',
    statement = SqlQueries.create_table_time,
    query = SqlQueries.time_table_insert
)

run_quality_checks = DataQualityOperator(
    task_id = 'Run_data_quality_checks',
    dag = dag,
    provide_context = True,
    aws_conn_id = "aws_credentials",
    redshift_conn_id = 'redshift',
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> [stage_events_to_redshift, stage_songs_to_redshift]
[stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table
load_songplays_table >> [load_song_dimension_table, load_user_dimension_table, load_artist_dimension_table, load_time_dimension_table]
[load_song_dimension_table, load_user_dimension_table, load_artist_dimension_table, load_time_dimension_table] >> run_quality_checks
run_quality_checks >> end_operator