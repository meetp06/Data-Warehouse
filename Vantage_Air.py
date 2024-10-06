from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import requests
import pandas as pd


def return_snowflake_conn():
    # Initialize the SnowflakeHook using Airflow's connection ID
    hook = SnowflakeHook(snowflake_conn_id='alpha_api_conn')  # Set this connection ID in Airflow
    return hook.get_conn().cursor()


@task
def extract(symbol: str, api_key: str):
    # Call Alpha Vantage API
    url = f'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&apikey={api_key}&outputsize=compact'
    response = requests.get(url)
    data = response.json()

    # Convert the JSON to DataFrame
    df = pd.DataFrame.from_dict(data['Time Series (Daily)'], orient='index')
    df.columns = ['open', 'high', 'low', 'close', 'volume']
    df.index.name = 'date'
    df = df.reset_index()
    df['symbol'] = symbol

    return df.to_dict(orient='records')  # Return data as list of records


@task
def filter_last_90_days(records):
    # Load DataFrame and filter for last 90 days
    df = pd.DataFrame(records)
    df['date'] = pd.to_datetime(df['date']).dt.date
    today = datetime.now().date()
    ninety_days_ago = today - timedelta(days=90)
    df_filtered = df[(df['date'] >= ninety_days_ago) & (df['date'] <= today)]
    
    return df_filtered.to_dict(orient='records')


@task
def load_to_snowflake(records, target_table):
    cur = return_snowflake_conn()

    try:
        cur.execute("BEGIN;")
        for record in records:
            # Check if record exists (idempotency)
            cur.execute("""
                SELECT COUNT(*) FROM {0}
                WHERE date = %s AND symbol = %s
            """.format(target_table), (record['date'], record['symbol']))

            record_exists = cur.fetchone()[0]
            if record_exists == 0:
                # If the record doesn't exist, insert it
                cur.execute("""
                    INSERT INTO {0} (date, open, high, low, close, volume, symbol)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """.format(target_table), 
                (record['date'], record['open'], record['high'], record['low'], record['close'], record['volume'], record['symbol']))

        cur.execute("COMMIT;")
        print("Data inserted successfully.")
    except Exception as e:
        cur.execute("ROLLBACK;")
        print(f"Error: {e}")
        raise e


with DAG(
    dag_id='stock_data_pipeline',
    start_date=days_ago(1),
    catchup=False,
    schedule_interval='@daily',
    tags=['stock', 'ETL'],
) as dag:
    
    # Fetch variables from Airflow for credentials and API information
    symbol = Variable.get("stock_symbol", default_var='AAPL')  # Fetch stock symbol, default is 'AAPL'
    api_key = Variable.get("alpha_vantage_api_key")  # Alpha Vantage API key stored in Airflow variables
    target_table = 'raw_data.stock_prices'
    
    # Define tasks
    raw_data = extract(symbol, api_key)
    filtered_data = filter_last_90_days(raw_data)
    load_to_snowflake(filtered_data, target_table)
