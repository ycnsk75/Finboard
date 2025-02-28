from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator
from airflow.models import Variable
from google.cloud import storage, bigquery
from google.oauth2 import service_account
from datetime import datetime, timedelta
import pandas as pd
import finnhub
import requests
import json
import time  # Already imported for previous time.sleep

# Configuration from airflow variables
KEY_JSON_FILE = Variable.get('KEY_JSON_FILE')  # Path to service account key JSON file
BUCKET_NAME = Variable.get('BUCKET_NAME')  # Default to projectbigquery
CRYPTO_FOLDER_NAME = Variable.get('CRYPTO_FOLDER_NAME') # Folder name for crypto data
STOCK_FOLDER_NAME = Variable.get('STOCK_FOLDER_NAME') # Folder name for stock data
COINCAP_FOLDER_NAME = Variable.get('COINCAP_FOLDER_NAME') # Folder name for CoinCap data
FINNHUB_FOLDER_NAME = Variable.get('FINNHUB_FOLDER_NAME') # Folder name for Finnhub data
API_KEY_FINHUB = Variable.get('API_KEY_FINHUB')  # Finnhub API key
COINCAP_API = Variable.get('COINCAP_API')  # CoinCap API URL
DBT_CLOUD_CONN_ID = Variable.get('DBT_CLOUD_CONN_ID') # Connection ID for dbt Cloud
DBT_CLOUD_JOB_ID = Variable.get('DBT_CLOUD_JOB_ID') # dbt Cloud job ID

# Validate required environment variables
if not all(
    [KEY_JSON_FILE, BUCKET_NAME, CRYPTO_FOLDER_NAME, STOCK_FOLDER_NAME, COINCAP_FOLDER_NAME, FINNHUB_FOLDER_NAME, API_KEY_FINHUB, COINCAP_API, DBT_CLOUD_CONN_ID, DBT_CLOUD_JOB_ID]
    ):
    raise ValueError("Missing required environment variables: KEY_JSON_FILE, BUCKET_NAME, CRYPTO_FOLDER_NAME, STOCK_FOLDER_NAME, COINCAP_FOLDER_NAME, FINNHUB_FOLDER_NAME, API_KEY_FINHUB, COINCAP_API, DBT_CLOUD_CONN_ID, DBT_CLOUD_JOB_ID")

# Dynamic schedule: 'None' for manual, '0 9 * * *' for daily 9 a.m.
SCHEDULE_INTERVAL = Variable.get("FETCH_API_SCHEDULE", default_var="0 9 * * *")

# Helper Functions
def gcp_client_auth():
    try:
        credentials = service_account.Credentials.from_service_account_info(json.loads(KEY_JSON_FILE))
        storage_client = storage.Client(credentials=credentials, project=credentials.project_id)
        bigquery_client = bigquery.Client(credentials=credentials, project=credentials.project_id)
        return storage_client, bigquery_client
    except Exception as e:
        print(f"Error connecting to Google Cloud: {e}")
        raise

def push_data_to_cs(storage_client, bucket_name, destination_blob_name, local_file=None, data=None):
    try:
        bucket = storage_client.bucket(bucket_name)
        full_blob_name = f"{destination_blob_name}"
        blob = bucket.blob(full_blob_name)
        if local_file:
            blob.upload_from_filename(local_file)
        elif data:
            blob.upload_from_string(data, content_type='application/json')
        print(f"File uploaded to gs://{bucket_name}/{full_blob_name}")
    except Exception as e:
        print(f"Error uploading to Cloud Storage: {e}")
        raise

def fetch_api_data(url):
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        print(f"Error fetching data from {url}: {e}")
        return None

# Initialize clients
storage_client, bigquery_client = gcp_client_auth()
finnhub_client = finnhub.Client(api_key=API_KEY_FINHUB)

def load_csv_to_gcs():
    """Load CSV files to GCS root"""
    csv_files = {
        "green_crypto.csv": "green_crypto.csv",
        "green_crypto_carbon.csv": "gren_crypto_carbon.csv",
        "green_stock.csv": "green_stock.csv",
        "green_stock_carbon.csv": "green_stock_carbon.csv",
    }
    for destination_blob_name, local_file in csv_files.items():
        push_data_to_cs(storage_client, BUCKET_NAME, destination_blob_name, local_file=local_file)

def fetch_finnhub_financials():
    """Fetch quote and market capitalization from Finnhub for green_stock companies and store in GCS raw/ folder"""
    green_stock_blob = storage_client.bucket(BUCKET_NAME).blob(f"{STOCK_FOLDER_NAME}/green_stock.csv" if STOCK_FOLDER_NAME else "green_stock.csv")
    green_stock_data = green_stock_blob.download_as_string().decode("utf-8")
    green_stock_df = pd.read_csv(pd.compat.StringIO(green_stock_data))
    tickers = green_stock_df["Ticker"].tolist()
    
    date_str = datetime.now().strftime("%Y-%m-%d")
    for ticker in tickers:
        try:
            # Fetch both quote and financials
            quote = finnhub_client.quote(ticker)
            basic_financial = finnhub_client.company_basic_financials(ticker, 'all')
            # Combine data into a single JSON object
            combined_data = {
                "ticker": ticker,
                "current_price": quote["c"],
                "marketCapitalization": basic_financial["metric"]["marketCapitalization"]
            }
            destination_blob_name = f"{FINNHUB_FOLDER_NAME}/{ticker}_financials_{date_str}.json"
            push_data_to_cs(storage_client, BUCKET_NAME, destination_blob_name, data=json.dumps(combined_data))
            time.sleep(0.5)  # 0.5-second delay between Finnhub API calls
        except Exception as e:
            print(f"Error fetching data for {ticker}: {e}")

def fetch_coincap_prices(CRYPTO_FOLDER_NAME):
    """Fetch price data from CoinCap for green_crypto coins and store in GCS raw/ folder"""
    green_crypto_blob = storage_client.bucket(BUCKET_NAME).blob(f"{CRYPTO_FOLDER_NAME}/green_crypto.csv" if CRYPTO_FOLDER_NAME else "green_crypto.csv")
    green_crypto_data = green_crypto_blob.download_as_string().decode("utf-8")
    green_crypto_df = pd.read_csv(pd.compat.StringIO(green_crypto_data))
    coins = green_crypto_df["Coin"].str.lower().tolist()
    
    date_str = datetime.now().strftime("%Y-%m-%d")
    for coin in coins:
        try:
            url = f"{COINCAP_API}{coin}"
            data = fetch_api_data(url)
            if data:
                minimal_data = {"coin": coin, "price_usd": float(data["data"]["priceUsd"])}
                destination_blob_name = f"{COINCAP_FOLDER_NAME}/{coin}_price_{date_str}.json"
                push_data_to_cs(storage_client, BUCKET_NAME, destination_blob_name, data=json.dumps(minimal_data))
                time.sleep(0.3)  # 0.3-second delay between CoinCap API calls
        except Exception as e:
            print(f"Error fetching price for {coin}: {e}")

def load_to_bigquery():
    """Load raw CSV and JSON data from GCS to BigQuery staging tables"""
    bq_tables = [
        # CSV Files from bucket root
        {
            "source": f"gs://{BUCKET_NAME}/{CRYPTO_FOLDER_NAME}/green_crypto.csv" if CRYPTO_FOLDER_NAME else f"gs://{BUCKET_NAME}/green_crypto.csv",
            "destination": "devops-practice-449210.finboard.raw_green_crypto",
            "schema": [
                {"name": "Coin", "type": "STRING"},
                {"name": "Symbol", "type": "STRING"},
                {"name": "Price", "type": "FLOAT", "mode": "NULLABLE"},
                {"name": "Change", "type": "FLOAT", "mode": "NULLABLE"},
                {"name": "Percent_Change", "type": "FLOAT", "mode": "NULLABLE"},
                {"name": "High_24h", "type": "FLOAT", "mode": "NULLABLE"},
                {"name": "Low_24h", "type": "FLOAT", "mode": "NULLABLE"},
                {"name": "Open_24h", "type": "FLOAT", "mode": "NULLABLE"},
                {"name": "Previous_Close_24h", "type": "FLOAT", "mode": "NULLABLE"},
                {"name": "Timestamp", "type": "INTEGER", "mode": "NULLABLE"}
            ],
            "source_format": "CSV",
            "skip_leading_rows": 1
        },
        {
            "source": f"gs://{BUCKET_NAME}/{CRYPTO_FOLDER_NAME}/green_crypto_carbon.csv" if CRYPTO_FOLDER_NAME else f"gs://{BUCKET_NAME}/green_crypto_carbon.csv",
            "destination": "devops-practice-449210.finboard.raw_green_crypto_carbon",
            "schema": [
                {"name": "Coin", "type": "STRING"},
                {"name": "Symbol", "type": "STRING"},
                {"name": "Type", "type": "STRING"},
                {"name": "Marketcap", "type": "STRING"},
                {"name": "Electrical_Power_GW", "type": "FLOAT", "mode": "NULLABLE"},
                {"name": "Electricity_Consumption_GW", "type": "FLOAT", "mode": "NULLABLE"},
                {"name": "CO2_Emissions_Mt", "type": "FLOAT", "mode": "NULLABLE"}
            ],
            "source_format": "CSV",
            "skip_leading_rows": 1
        },
        {
            "source": f"gs://{BUCKET_NAME}/{STOCK_FOLDER_NAME}/green_stock.csv" if STOCK_FOLDER_NAME else f"gs://{BUCKET_NAME}/green_stock.csv",
            "destination": "devops-practice-449210.finboard.raw_green_stock",
            "schema": [
                {"name": "ID", "type": "INTEGER"},
                {"name": "Company", "type": "STRING"},
                {"name": "Ticker", "type": "STRING"},
                {"name": "Category", "type": "STRING"},
                {"name": "Stock_Exchange", "type": "STRING"},
                {"name": "Marketcap", "type": "STRING"},
                {"name": "ESG_Score", "type": "INTEGER", "mode": "NULLABLE"},
                {"name": "Revenue", "type": "STRING"},
                {"name": "Main_Focus", "type": "STRING"},
                {"name": "Region", "type": "STRING"},
                {"name": "Year_Founded", "type": "INTEGER", "mode": "NULLABLE"}
            ],
            "source_format": "CSV",
            "skip_leading_rows": 1
        },
        {
            "source": f"gs://{BUCKET_NAME}/{STOCK_FOLDER_NAME}/green_stock_carbon.csv" if STOCK_FOLDER_NAME else f"gs://{BUCKET_NAME}/green_stock_carbon.csv",
            "destination": "devops-practice-449210.finboard.raw_green_stock_carbon",
            "schema": [
                {"name": "ID", "type": "INTEGER"},
                {"name": "Company", "type": "STRING"},
                {"name": "Ticker", "type": "STRING"},
                {"name": "Electricity_Consumption_GWh", "type": "FLOAT", "mode": "NULLABLE"},
                {"name": "CO2_Emissions_Mt", "type": "FLOAT", "mode": "NULLABLE"},
                {"name": "Electrical_Power_GWh", "type": "FLOAT", "mode": "NULLABLE"}
            ],
            "source_format": "CSV",
            "skip_leading_rows": 1
        },
        # API JSON Files in raw/ folder
        {
            "source": f"gs://{BUCKET_NAME}/{FINNHUB_FOLDER_NAME}/*_financials_{datetime.now().strftime('%Y-%m-%d')}.json" if FINNHUB_FOLDER_NAME else f"gs://{BUCKET_NAME}/raw/finnhub_financials/*_financials_{datetime.now().strftime('%Y-%m-%d')}.json",
            "destination": "devops-practice-449210.finboard.raw_finnhub_financials",
            "schema": [
                {"name": "ticker", "type": "STRING"},
                {"name": "current_price", "type": "FLOAT", "mode": "NULLABLE"},
                {"name": "marketCapitalization", "type": "FLOAT", "mode": "NULLABLE"}
            ],
            "source_format": "NEWLINE_DELIMITED_JSON",
            "skip_leading_rows": 0
        },
        {
            "source": f"gs://{BUCKET_NAME}/{COINCAP_FOLDER_NAME}/*_price_{datetime.now().strftime('%Y-%m-%d')}.json" if COINCAP_FOLDER_NAME else f"gs://{BUCKET_NAME}/raw/coincap_prices/*_price_{datetime.now().strftime('%Y-%m-%d')}.json",
            "destination": "devops-practice-449210.finboard.raw_coincap_prices",
            "schema": [
                {"name": "coin", "type": "STRING"},
                {"name": "price_usd", "type": "FLOAT", "mode": "NULLABLE"}
            ],
            "source_format": "NEWLINE_DELIMITED_JSON",
            "skip_leading_rows": 0
        }
    ]
    
    for config in bq_tables:
        job_config = bigquery.LoadJobConfig(
            source_format=config["source_format"],
            skip_leading_rows=config["skip_leading_rows"],
            schema=config["schema"],
            write_disposition="WRITE_TRUNCATE",
            autodetect=False,
            null_marker=""  # Treat empty strings as NULL for CSVs
        )
        load_job = bigquery_client.load_table_from_uri(
            config["source"], config["destination"], job_config=job_config
        )
        load_job.result()
        print(f"Loaded data into {config['destination']}")

with DAG(
    "elt_pipeline",
    start_date=datetime(2025, 2, 1),
    schedule_interval=SCHEDULE_INTERVAL,
    catchup=False,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    }
) as dag:
    
    fetch_finnhub = PythonOperator(
        task_id="fetch_finnhub_financials",
        python_callable=fetch_finnhub_financials
    )
    
    fetch_coincap = PythonOperator(
        task_id="fetch_coincap_prices",
        python_callable=fetch_coincap_prices
    )
    
    load_to_bq = PythonOperator(
        task_id="load_to_bigquery",
        python_callable=load_to_bigquery
    )
    
    run_dbt_cloud_job = DbtCloudRunJobOperator(
        task_id="run_dbt_cloud_job",
        dbt_cloud_conn_id=DBT_CLOUD_CONN_ID,
        job_id=DBT_CLOUD_JOB_ID,
        wait_for_termination=True,
        timeout=60 * 60 * 24 * 7,  # 7 days timeout
        check_interval=60,
        trigger_reason="Triggered by Airflow to calculate green efficiency scores",
        additional_run_config={"threads": 4}
    )
    
    [fetch_finnhub, fetch_coincap] >> load_to_bq >> run_dbt_cloud_job