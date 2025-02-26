from google.cloud import bigquery
import json
import requests
from google.cloud import storage
import pandas as pd
import os 

# connexion à api google cloud par la clé SA (clé à copier sous le répertoire credentials/)
def gcp_client_auth(key_json_file):
    try:
        storage_client = storage.Client.from_service_account_json(key_json_file)
        bigquery_client = bigquery.Client.from_service_account_json(key_json_file)  
        return storage_client #, bigquery_client
    except Exception as e:
        print(f"Error connecting to Google Cloud: {e}")
        exit(1) # Exit with an error code

def import_data_to_bq_from_cs(bigquery_client, table_name, bucket_name, blob_name, table_id):
    job_config = bigquery.LoadJobConfig(
    autodetect=True, source_format=bigquery.SourceFormat.CSV, skip_leading_rows=1, write_disposition=bigquery.WriteDisposition.WRITE_APPEND
    )
    uri = "gs://{bucket_name}/{blob_name}"
    # gs://projectbigquery/data/train.csv
    uri = uri.format(bucket_name=bucket_name, blob_name=blob_name)
    load_job = bigquery_client.load_table_from_uri(
        uri, table_id, job_config=job_config
    )  # Make an API request.
    load_job.result()  # Waits for the job to complete.
    destination_table = bigquery_client.get_table(table_id)
    print("Loaded {} rows.".format(destination_table.num_rows))

def get_data_from_bq(bigquery_client, table_name):
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
    
# Variables, Make sure these match your project and dataset.
key_json_file = os.getenv('KEY_JSON_FILE')
bucket_name = os.getenv('BUCKET_NAME')
folder_name = os.getenv('FOLDER_NAME')
# gcp client auth  add variable after comment for bq usage# , bigquery_client
storage_client, bigquery_client = gcp_client_auth(key_json_file)

# export raw data to CS
#push_data_to_cs(storage_client, bucket_name, raw_data_file_name, table_name)

# import data to bq from cs
#import_data_to_bq_from_cs(bigquery_client, table_name)

# read data from bq
#get_data_from_bq(bigquery_client, table_name)

# export processed data to CS
#push_data_to_cs(storage_client, bucket_name, processed_data_file_name, table_name)

# export processed data to CS
#push_data_to_cs(storage_client, bucket_name, pickle_file_name, table_name)

#import_data_to_bq_from_cs(bigquery_client, table_name, bucket_name, blob_name, table_id)
csv_files = {
    "raw/crypto/green_crypto.csv": "data/green_crypto.csv",
    "raw/stock/green_stock.csv": "data/green_stock.csv",
    "raw/crypto/green_crypto_carbon.csv":"data/green_crypto_carbon.csv",
    "raw/crypto/green_stock_carbon.csv":"data/green_stock_carbon.csv",

}

endpoints = ["assets", "rates", "exchanges", "markets", "candles"]

api_endpoints = {
    "raw/crypto/assets.json": "https://api.coincap.io/v2/assets",
    "raw/crypto/rates.json": "https://api.coincap.io/v2/rates",
    "raw/crypto/exchanges.json": "https://api.coincap.io/v2/exchanges",
    "raw/crypto/markets.json": "https://api.coincap.io/v2/markets",
    "raw/crypto/candles.json": "https://api.coincap.io/v2/candles",
}

#get_cloud_storage_contents(storage_client, bucket_name)
# Upload CSV files
to_upload = [(dest, src) for dest, src in csv_files.items()]
for destination_blob_name, local_file in to_upload:
    push_data_to_cs(storage_client, bucket_name, destination_blob_name, local_file)

# Fetch API data and upload to Cloud Storage

# for destination_blob_name, url in api_endpoints.items(): 
#     data = fetch_api_data(url)
    
#     if data:  # Ensure we have valid data
#         json_data = json.dumps(data)  # Convert dictionary to JSON string
#         push_data_to_cs(storage_client, bucket_name, destination_blob_name, data=json_data)
import_data_to_bq_from_cs