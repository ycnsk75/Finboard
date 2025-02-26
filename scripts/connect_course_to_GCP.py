from google.cloud import bigquery
import json
import requests
from google.cloud import storage
import pandas as pd
import os 

# Authenticate with Google Cloud using a service account key.
def gcp_client_auth(key_json_file):
    try:
        storage_client = storage.Client.from_service_account_json(key_json_file)
        bigquery_client = bigquery.Client.from_service_account_json(key_json_file)  
        return storage_client, bigquery_client
    except Exception as e:
        print(f"Error connecting to Google Cloud: {e}")
        exit(1)  # Exit with an error code

# Load CSV data from Cloud Storage into BigQuery.
def import_data_to_bq_from_cs(bigquery_client, table_name, bucket_name, blob_name, table_id):
    job_config = bigquery.LoadJobConfig(
        autodetect=True,
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
    )
    
    uri = f"gs://{bucket_name}/{blob_name}"
    load_job = bigquery_client.load_table_from_uri(
        uri, table_id, job_config=job_config
    )
    load_job.result()  # Wait for the job to complete.
    destination_table = bigquery_client.get_table(table_id)
    print("Loaded {} rows into table {}.".format(destination_table.num_rows, table_id))

def get_data_from_bq(bigquery_client, table_name):
    # This function assumes you have variables 'project' and 'dataset_id' defined.
    dataset_ref = bigquery.DatasetReference(project, dataset_id)
    table_ref = dataset_ref.table(table_name)
    print("Table reference:", table_ref)
    df = bigquery_client.list_rows(table_ref).to_dataframe()
    print("Data frame: ", df)
    return df

def push_data_to_cs(storage_client, bucket_name, destination_blob_name, local_file=None, data=None):
    try:
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)
        
        if local_file:
            blob.upload_from_filename(local_file)
        elif data:
            blob.upload_from_string(data, content_type='application/json')
        
        print(f"File uploaded to gs://{bucket_name}/{destination_blob_name}")
    except Exception as e:
        print(f"Error uploading to Cloud Storage: {e}")

def get_cloud_storage_contents(storage_client, bucket_name):
    try:
        bucket = storage_client.bucket(bucket_name)
        blobs = list(bucket.list_blobs())
        print("Files in Cloud Storage:")
        for blob in blobs:
            print(blob.name)
        return blobs
    except Exception as e:
        print(f"Error retrieving Cloud Storage contents: {e}")
        return []
    
def fetch_api_data(url):
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        print(f"Error fetching data from {url}: {e}")
        return None

# Variables – make sure these match your project and dataset.
key_json_file = os.getenv('KEY_JSON_FILE')
bucket_name = os.getenv('BUCKET_NAME')
folder_name = os.getenv('FOLDER_NAME')
# Authenticate with Google Cloud (returns both storage and BigQuery clients)
storage_client, bigquery_client = gcp_client_auth(key_json_file)

# Dictionary mapping destination blob names (GCS path) to local CSV file paths.
csv_files = {
    "raw/crypto/green_crypto.csv": "../data/green_crypto.csv",
    "raw/stock/green_stock.csv": "../data/green_stock.csv",
    "raw/crypto/green_crypto_carbon.csv": "../data/green_crypto_carbon.csv",
    "raw/crypto/green_stock_carbon.csv": "../data/green_stock_carbon.csv",
}

# (Optional) API endpoints mapping, if needed.
api_endpoints = {
    "raw/crypto/assets.json": "https://api.coincap.io/v2/assets",
    "raw/crypto/rates.json": "https://api.coincap.io/v2/rates",
    "raw/crypto/exchanges.json": "https://api.coincap.io/v2/exchanges",
    "raw/crypto/markets.json": "https://api.coincap.io/v2/markets",
    "raw/crypto/candles.json": "https://api.coincap.io/v2/candles",
}

# 1. Upload CSV files from local paths to Google Cloud Storage.
for destination_blob_name, local_file in csv_files.items():
    push_data_to_cs(storage_client, bucket_name, destination_blob_name, local_file)

# 2. Define mapping from the destination blob names to their respective BigQuery table IDs.
# For example, if your project is "devops-practice-449210" and your dataset is "finboard",
# then a table ID for green_crypto might be: devops-practice-449210.finboard.green_crypto
bq_tables = {
    "raw/crypto/green_crypto.csv": "devops-practice-449210.finboard.green_crypto",
    "raw/stock/green_stock.csv": "devops-practice-449210.finboard.green_stock",
    "raw/crypto/green_crypto_carbon.csv": "devops-practice-449210.finboard.green_crypto_carbon",
    "raw/crypto/green_stock_carbon.csv": "devops-practice-449210.finboard.green_stock_carbon",
}

# 3. Load each CSV file from Cloud Storage into BigQuery.
for blob_name, table_id in bq_tables.items():
    import_data_to_bq_from_cs(bigquery_client, table_id, bucket_name, blob_name, table_id)

# (Optional) If you want to fetch API data and upload it to Cloud Storage, you can uncomment and use:
# for destination_blob_name, url in api_endpoints.items(): 
#     data = fetch_api_data(url)
#     if data:  # Ensure we have valid data
#         json_data = json.dumps(data)
#         push_data_to_cs(storage_client, bucket_name, destination_blob_name, data=json_data)
