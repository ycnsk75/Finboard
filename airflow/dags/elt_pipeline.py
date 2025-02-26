from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from google.cloud import storage, bigquery
from datetime import datetime, timedelta
from dotenv import load_dotenv
import os
import pandas as pd
import finnhub
import requests
import json

load_dotenv()

SERVICE_ACCOUNT_KEY = "/home/airflow/gcs/data/devops-practice-python.json"
FINNHUB_API_KEY = "cutr999r01qv6ijjvqn0cutr999r01qv6ijjvqng"
finnhub_client = finnhub.Client(api_key=FINNHUB_API_KEY)
COINCAP_API = "https://api.coincap.io/v2/assets/"

def load_csv_to_gcs():
    """Load CSV files to GCS"""
    storage_client = storage.Client.from_service_account_json(SERVICE_ACCOUNT_KEY)
    bucket = storage_client.bucket("green-investment-raw-data")
    
    csv_files = [
        "green_crypto.csv",
        "green_crypto_carbon.csv",
        "green_stock.csv",
        "green_stock_carbon.csv"
    ]
    
    for csv_file in csv_files:
        blob = bucket.blob(f"raw/csv/{csv_file}")
        blob.upload_from_filename(csv_file)

def fetch_finnhub_quotes():
    """Fetch quote data (current price only) from Finnhub for green_stock companies"""
    storage_client = storage.Client.from_service_account_json(SERVICE_ACCOUNT_KEY)
    bucket = storage_client.bucket("green-investment-raw-data")
    
    green_stock_blob = bucket.blob("raw/csv/green_stock.csv")
    green_stock_data = green_stock_blob.download_as_string().decode("utf-8")
    green_stock_df = pd.read_csv(pd.compat.StringIO(green_stock_data))
    tickers = green_stock_df["Ticker"].tolist()
    
    date_str = datetime.now().strftime("%Y-%m-%d")
    for ticker in tickers:
        try:
            quote = finnhub_client.quote(ticker)
            minimal_quote = {"ticker": ticker, "current_price": quote["c"]}
            blob = bucket.blob(f"raw/finnhub_quotes/{ticker}_quote_{date_str}.json")
            blob.upload_from_string(json.dumps(minimal_quote))
        except Exception as e:
            print(f"Error fetching quote for {ticker}: {e}")

def fetch_coincap_prices():
    """Fetch price data from CoinCap for green_crypto coins"""
    storage_client = storage.Client.from_service_account_json(SERVICE_ACCOUNT_KEY)
    bucket = storage_client.bucket("green-investment-raw-data")
    
    # Read green_crypto.csv from GCS
    green_crypto_blob = bucket.blob("raw/csv/green_crypto.csv")
    green_crypto_data = green_crypto_blob.download_as_string().decode("utf-8")
    green_crypto_df = pd.read_csv(pd.compat.StringIO(green_crypto_data))
    coins = green_crypto_df["Coin"].str.lower().tolist()  # CoinCap uses lowercase IDs
    
    date_str = datetime.now().strftime("%Y-%m-%d")
    for coin in coins:
        try:
            url = f"{COINCAP_API}{coin}"
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()["data"]
            # Only keep what we need (coin ID and priceUsd)
            minimal_data = {
                "coin": coin,
                "price_usd": float(data["priceUsd"])
            }
            blob = bucket.blob(f"raw/coincap_prices/{coin}_price_{date_str}.json")
            blob.upload_from_string(json.dumps(minimal_data))
        except Exception as e:
            print(f"Error fetching price for {coin}: {e}")

def load_csv_to_bigquery():
    """Load raw CSV and JSON data from GCS to BigQuery staging tables"""
    bq_client = bigquery.Client.from_service_account_json(SERVICE_ACCOUNT_KEY)
    
    csv_configs = [
        {
            "source": "gs://green-investment-raw-data/raw/csv/green_crypto.csv",
            "destination": "green-investment.staging.raw_green_crypto",
            "schema": [
                {"name": "Coin", "type": "STRING"},
                {"name": "Symbol", "type": "STRING"},
                {"name": "Price", "type": "FLOAT"},
                {"name": "Change", "type": "FLOAT"},
                {"name": "Percent_Change", "type": "FLOAT"},
                {"name": "High_24h", "type": "FLOAT"},
                {"name": "Low_24h", "type": "FLOAT"},
                {"name": "Open_24h", "type": "FLOAT"},
                {"name": "Previous_Close_24h", "type": "FLOAT"},
                {"name": "Timestamp", "type": "INTEGER"}
            ]
        },
        {
            "source": "gs://green-investment-raw-data/raw/csv/green_crypto_carbon.csv",
            "destination": "green-investment.staging.raw_green_crypto_carbon",
            "schema": [
                {"name": "Coin", "type": "STRING"},
                {"name": "Symbol", "type": "STRING"},
                {"name": "Type", "type": "STRING"},
                {"name": "Marketcap", "type": "FLOAT"},
                {"name": "Electrical_Power", "type": "FLOAT"},
                {"name": "Electricity_Consumption_annualised", "type": "FLOAT"},
                {"name": "CO2_Emissions_annualised", "type": "FLOAT"}
            ]
        },
        {
            "source": "gs://green-investment-raw-data/raw/csv/green_stock.csv",
            "destination": "green-investment.staging.raw_green_stock",
            "schema": [
                {"name": "ID", "type": "INTEGER"},
                {"name": "Company", "type": "STRING"},
                {"name": "Ticker", "type": "STRING"},
                {"name": "Category", "type": "STRING"},
                {"name": "Stock_Exchange", "type": "STRING"},
                {"name": "Marketcap_BUSD", "type": "FLOAT"},
                {"name": "ESG_Score", "type": "INTEGER"},
                {"name": "Revenue_BUSD", "type": "FLOAT"},
                {"name": "Main_Focus", "type": "STRING"},
                {"name": "Region", "type": "STRING"},
                {"name": "Year_Founded", "type": "INTEGER"}
            ]
        },
        {
            "source": "gs://green-investment-raw-data/raw/csv/green_stock_carbon.csv",
            "destination": "green-investment.staging.raw_green_stock_carbon",
            "schema": [
                {"name": "ID", "type": "INTEGER"},
                {"name": "Company", "type": "STRING"},
                {"name": "Ticker", "type": "STRING"},
                {"name": "Electricity_Consumption_GWh", "type": "FLOAT"},
                {"name": "CO2_Emissions_million_tons", "type": "FLOAT"},
                {"name": "Electrical_Power", "type": "FLOAT"}
            ]
        },
        {
            "source": "gs://green-investment-raw-data/raw/finnhub_quotes/*_quote_*.json",
            "destination": "green-investment.staging.raw_finnhub_quotes",
            "schema": [
                {"name": "ticker", "type": "STRING"},
                {"name": "current_price", "type": "FLOAT"}
            ]
        },
        {
            "source": "gs://green-investment-raw-data/raw/coincap_prices/*_price_*.json",
            "destination": "green-investment.staging.raw_coincap_prices",
            "schema": [
                {"name": "coin", "type": "STRING"},
                {"name": "price_usd", "type": "FLOAT"}
            ]
        }
    ]
    
    for config in csv_configs:
        job_config = bigquery.LoadJobConfig(
            source_format="CSV" if config["source"].endswith(".csv") else "NEWLINE_DELIMITED_JSON",
            skip_leading_rows=1 if config["source"].endswith(".csv") else 0,
            schema=config["schema"],
            write_disposition="WRITE_TRUNCATE",
            autodetect=False
        )
        bq_client.load_table_from_uri(
            config["source"],
            config["destination"],
            job_config=job_config
        ).result()

with DAG(
    "elt_green_data",
    start_date=datetime(2025, 2, 1),
    schedule_interval="0 0 1 * *",
    catchup=False
) as dag:
    upload_to_gcs = PythonOperator(
        task_id="load_csv_to_gcs",
        python_callable=load_csv_to_gcs
    )
    
    fetch_finnhub = PythonOperator(
        task_id="fetch_finnhub_quotes",
        python_callable=fetch_finnhub_quotes
    )
    
    fetch_coincap = PythonOperator(
        task_id="fetch_coincap_prices",
        python_callable=fetch_coincap_prices
    )
    
    load_to_bq = PythonOperator(
        task_id="load_csv_to_bigquery",
        python_callable=load_csv_to_bigquery
    )
    
    run_dbt = BashOperator(
        task_id="run_dbt_transformations",
        bash_command="cd /path/to/dbt/project && dbt run --profiles-dir ."
    )
    
    upload_to_gcs >> [fetch_finnhub, fetch_coincap] >> load_to_bq >> run_dbt