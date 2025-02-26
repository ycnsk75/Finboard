import finnhub
import json
import os
import pandas as pd
from datetime import datetime, timedelta
from google.cloud import storage


### Pour avoir list sympbol dynamique : -> require stocks_symbol (list symbol par currency -> pour marcher US renseigner 
# finnhub_client.stock_symbols('US') Puis boucler sur top 20 symbol pour avoir les données
# )
# --- Google Cloud Storage Helper Functions ---
def gcp_client_auth(key_json_file):
    try:
        storage_client = storage.Client.from_service_account_json(key_json_file)
        # bigquery_client = bigquery.Client.from_service_account_json(key_json_file)  
        return storage_client  # , bigquery_client
    except Exception as e:
        print(f"Error connecting to Google Cloud: {e}")
        exit(1)  # Exit with an error code

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

# --- Configuration Variables ---
KEY_JSON_FILE = os.getenv('KEY_JSON_FILE')  # Path to your service account key JSON file
BUCKET_NAME = os.getenv('BUCKET_NAME')       # Your GCS bucket name
FOLDER_NAME = os.getenv('FOLDER_NAME')       # Optional folder within the bucket
API_KEY_FINHUB = os.getenv('API_KEY_FINHUB') # Your Finnhub API key
# Initialize the GCP storage client
storage_client = gcp_client_auth(KEY_JSON_FILE)
print(storage_client)
print(BUCKET_NAME)
#test bucket 
get_cloud_storage_contents(storage_client, BUCKET_NAME)
# --- Finnhub API Data Fetching ---
# Setup your Finnhub client
finnhub_client = finnhub.Client(api_key=API_KEY_FINHUB)

# # Load the list of stock symbols from the top_200stocks.txt file
with open("../data/top_200_stocks.txt", "r") as file:
    symbols = [line.strip() for line in file if line.strip()]

# # Define a date range for fetching recent news (last 10 days)
today = datetime.today()
news_from = (today - timedelta(days=10)).strftime('%Y-%m-%d')
news_to = today.strftime('%Y-%m-%d')

# Dictionary to hold all stock data
all_stock_data = {}
ipo_calendar = None

try:
# Loop over each symbol and fetch data from the allowed endpoints
    for symbol in symbols:
        stock_data = {}
        try:
             # Company Profile 2: Basic company info
            profile = finnhub_client.company_profile2(symbol=symbol)
            stock_data['profile'] = profile

             # Real-time Quote: Price and related metrics
            quote = finnhub_client.quote(symbol)
            stock_data['quote'] = quote

             # Recommendation Trends: Analyst recommendations
            rec_trends = finnhub_client.recommendation_trends(symbol)
            stock_data['recommendation_trends'] = rec_trends

            # Basic Financials: Financial summary data
            basic_financials = finnhub_client.company_basic_financials(symbol, 'all')
            stock_data['basic_financials'] = basic_financials

             # Company News: Recent news using a dynamic date range
            news = finnhub_client.company_news(symbol, _from=news_from, to=news_to)
            stock_data['news'] = news

        except Exception as e:
            print(f"Error fetching data for {symbol}: {e}")
             # Even if an endpoint fails for a symbol, we store what we got so far.
        
        all_stock_data[symbol] = stock_data

     # Fetch IPO calendar data for a given period
    try:
        ipo_calendar = finnhub_client.ipo_calendar(_from="2020-05-01", to="2020-06-01")
    except Exception as e:
        print(f"Error fetching IPO calendar: {e}")

#     # Consolidate all data into a single dictionary
    final_data = {
        "stocks": all_stock_data,
        "ipo_calendar": ipo_calendar
    }
    
except Exception as e:
    print(f"Unexpected error occurred during data collection: {e}")
     # Even if something unexpected happens, proceed with what has been collected.
    final_data = {
        "stocks": all_stock_data,
        "ipo_calendar": ipo_calendar
    }
    
finally:
     # Save the collected data to a JSON file locally
    output_file = "../data/stock_data_finhub.json"
    try:
        with open(output_file, "w") as outfile:
            json.dump(final_data, outfile, indent=4)
        print("Data saved locally to", output_file)
    except Exception as e:
        print(f"Error saving data locally: {e}")
        
      # Specify the destination path exactly as desired in GCS
    destination_blob_name = "raw/stock/finhub_stock_data.json"
    
     # Upload the JSON file to Google Cloud Storage
    push_data_to_cs(storage_client, BUCKET_NAME, destination_blob_name, local_file=output_file)
