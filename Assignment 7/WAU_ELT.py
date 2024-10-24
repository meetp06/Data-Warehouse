from airflow import DAG
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime
import logging 

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1
}

# Define the DAG
with DAG(
    'elt_create_joined_session_summary_dev_analytics',
    default_args=default_args,
    description='ELT DAG to join user_session_channel and session_timestamp into session_summary in dev.analytics schema',
    schedule_interval=None,
    catchup=False
) as dag:

    @task
    def create_session_summary_table():
        """Create the session_summary table in the dev.analytics schema."""
        snowflake_hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
        sql = """
        CREATE TABLE IF NOT EXISTS dev.analytics.session_summary (
            sessionId varchar(32) primary key,
            userId int,
            channel varchar(32),
            ts timestamp
        );
        """
        snowflake_hook.run(sql)

    
    @task
    def transform_and_load_data():
        """Perform the JOIN operation, check for duplicates using DISTINCT, and load into session_summary table."""
        snowflake_hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')

        sql = """
        INSERT INTO dev.analytics.session_summary (sessionId, userId, channel, ts)
        SELECT DISTINCT
            usc.sessionId,
            usc.userId,
            usc.channel,
            st.ts
        FROM dev.raw_data.user_session_channel usc
        JOIN dev.raw_data.session_timestamp st
        ON usc.sessionId = st.sessionId
        WHERE st.ts = (SELECT MAX(ts) FROM dev.raw_data.session_timestamp WHERE sessionId = usc.sessionId);
        """

        # Execute the combined insert and select statement
        snowflake_hook.run(sql)

        logging.info("Successfully completed the data transformation and load process.")


   

    # Task dependencies
    create_session_summary = create_session_summary_table()
    transform_and_load = transform_and_load_data()

    create_session_summary >> transform_and_load
