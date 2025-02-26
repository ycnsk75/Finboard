from airflow import DAG
from airflow.operators.python import PythonOperator
from google.cloud import storage, bigquery
import pandas as pd
import requests
from datetime import datetime, timedelta
from dotenv import load_dotenv
import os
import finnhub

# Load environment variables from .env file
load_dotenv()

# API keys from .env
FINNHUB_API_KEY = os.getenv("FINNHUB_API_KEY")
COINCAP_API = "https://api.coincap.io/v2/assets/"

# Path to service account key
SERVICE_ACCOUNT_KEY = "/home/airflow/gcs/data/devops-practice-python.json"

# Initialize Finnhub client
finnhub_client = finnhub.Client(api_key=FINNHUB_API_KEY)

def fetch_finnhub_historical(ticker):
    # Fetch 1 year of daily data (Unix timestamps in seconds)
    end = int(datetime.now().timestamp())
    start = int((datetime.now() - timedelta(days=365)).timestamp())
    data = finnhub_client.stock_candles(ticker, 'D', start, end)
    df = pd.DataFrame({
        "date": [datetime.fromtimestamp(ts).strftime('%Y-%m-%d') for ts in data["t"]],
        "price": data["c"],  # Closing price
        "ticker": [ticker] * len(data["t"])
    })
    return df

def fetch_coincap_historical(coin):
    url = f"https://api.coincap.io/v2/assets/{coin}/history?interval=d1&start={(int((datetime.now() - timedelta(days=365)).timestamp() * 1000))}&end={(int(datetime.now().timestamp() * 1000))}"
    resp = requests.get(url).json()
    data = [
        {"date": datetime.fromtimestamp(int(item["time"]) / 1000).strftime('%Y-%m-%d'), "price": float(item["priceUsd"]), "ticker": coin}
        for item in resp.get("data", [])
    ]
    return pd.DataFrame(data)

def fetch_and_load_historical():
    storage_client = storage.Client.from_service_account_json(SERVICE_ACCOUNT_KEY)
    bq_client = bigquery.Client.from_service_account_json(SERVICE_ACCOUNT_KEY)
    bucket = storage_client.bucket("green-investment-raw-data")

    # Load CSVs
    stocks_df = pd.read_csv(bucket.blob("green_stocks.csv").download_as_bytes().decode("utf-8"))
    crypto_df = pd.read_csv(bucket.blob("green_crypto.csv").download_as_bytes().decode("utf-8"))

    bq_client.load_table_from_dataframe(
        stocks_df, "green-investment.green_dataset.green_stocks",
        job_config=bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    ).result()
    bq_client.load_table_from_dataframe(
        crypto_df, "green-investment.green_dataset.green_crypto",
        job_config=bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    ).result()

    # Fetch historical data for 1 year
    date_str = datetime.now().strftime("%Y-%m-%d")
    for ticker in stocks_df["Ticker"]:
        df = fetch_finnhub_historical(ticker)
        blob = bucket.blob(f"historical_stocks_{ticker}_{date_str}.csv")
        blob.upload_from_string(df.to_csv(index=False))

    for coin in crypto_df["Coin"].str.lower():
        df = fetch_coincap_historical(coin)
        blob = bucket.blob(f"historical_crypto_{coin}_{date_str}.csv")
        blob.upload_from_string(df.to_csv(index=False))

    # Load to BigQuery
    for blob in bucket.list_blobs(prefix="historical_stocks"):
        bq_client.load_table_from_uri(
            f"gs://green-investment-raw-data/{blob.name}",
            "green-investment.green_dataset.historical_stocks_raw",
            job_config=bigquery.LoadJobConfig(
                write_disposition="WRITE_TRUNCATE",
                source_format="CSV",
                skip_leading_rows=1
            )
        ).result()

    for blob in bucket.list_blobs(prefix="historical_crypto"):
        bq_client.load_table_from_uri(
            f"gs://green-investment-raw-data/{blob.name}",
            "green-investment.green_dataset.historical_crypto_raw",
            job_config=bigquery.LoadJobConfig(
                write_disposition="WRITE_TRUNCATE",
                source_format="CSV",
                skip_leading_rows=1
            )
        ).result()

with DAG(
    "fetch_historical_data",
    start_date=datetime(2025, 2, 1),
    schedule_interval="0 0 1 * *",  # Monthly on the 1st
    catchup=False
) as dag:
    fetch_task = PythonOperator(
        task_id="fetch_and_load_historical",
        python_callable=fetch_and_load_historical
    )