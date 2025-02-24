import requests
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os

API_KEY_CARBONAPI = os.getenv("API_KEY_CARBONAPI")
OUTPUT_FILE = "network_data.txt"
HEADERS = {
    "Authorization": f"Bearer {API_KEY_CARBONAPI}",
}

# 1️⃣ Fetch available tickers
def fetch_tickers():
    url = "https://v2.api.carbon-ratings.com/currencies"
    response = requests.get(url, headers=HEADERS)
    
    try:
        data = response.json()
        if isinstance(data, list):  # Ensure it's a list of tickers
            return data
        else:
            print("❌ Unexpected response format:", data)
            return []
    except requests.exceptions.JSONDecodeError:
        print("❌ Failed to parse response:", response.text)
        return []

# 2️⃣ Fetch detailed network data for a given currency
def fetch_network_data(ticker):
    urls = {
        "power": f"https://v2.api.carbon-ratings.com/currencies/{ticker}/power/network",
        "electricity": f"https://v2.api.carbon-ratings.com/currencies/{ticker}/electricity-consumption/network",
        "emissions": f"https://v2.api.carbon-ratings.com/currencies/{ticker}/emissions/network",
    }

    data = {"ticker": ticker}
    
    for key, url in urls.items():
        response = requests.get(url, headers=HEADERS)
        
        try:
            data[key] = response.json()
        except requests.exceptions.JSONDecodeError:
            print(f"❌ Failed to parse {key} data for {ticker}: {response.text}")
            data[key] = None  # Store None if there's an issue

    return data

# 3️⃣ Analyze top 3 currencies and save results
def analyze_top_currencies(top_n=3):
    tickers = fetch_tickers()
    
    if not tickers:
        print("❌ No tickers found!")
        return None

    top_currencies = tickers[:top_n]  # Take the first N tickers
    results = [fetch_network_data(ticker) for ticker in top_currencies]

    # Save to .txt file with \n delimiter
    with open(OUTPUT_FILE, "w") as file:
        for entry in results:
            file.write(str(entry) + "\n")  # Convert dict to string and write

    print(f"✅ Data saved to {OUTPUT_FILE}")

    return results

# Run analysis
df_results = analyze_top_currencies()

def fetch_ratings_raw():
    url = "https://v2.api.carbon-ratings.com/currencies"
    response = requests.get(url, headers=HEADERS)

    print("Raw Response:", response.text)  # Debugging

    return response.json()  # This line fails if the response is not JSON

#ratings_data_raw = fetch_ratings_raw()
