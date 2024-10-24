from airflow import DAG
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1
}

# Define the DAG
with DAG(
    'create_and_populate_snowflake_tables',
    default_args=default_args,
    description='DAG to create and populate tables in Snowflake using task decorator',
    schedule_interval=None,
    catchup=False
) as dag:

    @task
    def create_user_session_channel_table():
        """Create user_session_channel table in Snowflake."""
        snowflake_hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
        sql = """
        CREATE TABLE IF NOT EXISTS dev.raw_data.user_session_channel (
            userId int not NULL,
            sessionId varchar(32) primary key,
            channel varchar(32) default 'direct'
        );
        """
        snowflake_hook.run(sql)

    @task
    def create_session_timestamp_table():
        """Create session_timestamp table in Snowflake."""
        snowflake_hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
        sql = """
        CREATE TABLE IF NOT EXISTS dev.raw_data.session_timestamp (
            sessionId varchar(32) primary key,
            ts timestamp
        );
        """
        snowflake_hook.run(sql)

    @task
    def copy_user_session_channel_data():
        """Populate user_session_channel table from S3 into Snowflake."""
        snowflake_hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
        sql = """
        CREATE OR REPLACE STAGE dev.raw_data.blob_stage
        url = 's3://s3-geospatial/readonly/'
        file_format = (type = csv, skip_header = 1, field_optionally_enclosed_by = '"');
        
        COPY INTO dev.raw_data.user_session_channel
        FROM @dev.raw_data.blob_stage/user_session_channel.csv;
        """
        snowflake_hook.run(sql)

    @task
    def copy_session_timestamp_data():
        """Populate session_timestamp table from S3 into Snowflake."""
        snowflake_hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
        sql = """
        COPY INTO dev.raw_data.session_timestamp
        FROM @dev.raw_data.blob_stage/session_timestamp.csv;
        """
        snowflake_hook.run(sql)

    # Task dependencies
    create_user_session_channel_table() >> create_session_timestamp_table() >> [copy_user_session_channel_data(), copy_session_timestamp_data()]

