import requests
import os 
import json
import time
# API endpoints
api_endpoints = {
    "crypto": "https://api.coincap.io/v2/assets/",
    "stock": "https://www.alphavantage.co/query"
}

# Default API key for AlphaVantage (Replace with your actual key)
ALPHA_VANTAGE_API_KEY = os.getenv("API_KEY_ALPHAVANTAGE")

def fetch_data(api_name, params=None):
    """Fetch data from the selected API with optional parameters."""
    url = api_endpoints.get(api_name)
    if not url:
        print(f"API '{api_name}' not found.")
        return None

    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        print(f"Error fetching data from {api_name}: {e}")
        return None

def get_api_info(api_name):
    """Return API documentation or helpful information."""
    api_docs = {
        "crypto": "https://docs.coincap.io/",
        "stock": "https://www.alphavantage.co/documentation/"
    }
    return api_docs.get(api_name, "No documentation available.")

# Example Usage
if __name__ == "__main__":
    STOCKS_FILE = "data/top_200_stocks.txt"
    OUTPUT_JSON = "data/stock_data.json"
    TEMP_JSON = "data/stock_data_tmp.json"
    
    # List of top 200 biggest stocks (Replace with a real updated list)
    top_200_stocks = [
        "AAPL", "MSFT", "GOOGL", "AMZN", "TSLA", "NVDA", "BRK.A", "META", "V", "JNJ",
        "WMT", "UNH", "JPM", "PG", "XOM", "HD", "MA", "LLY", "PFE", "ABBV",
        "COST", "KO", "PEP", "MRK", "AVGO", "DIS", "CVX", "ADBE", "NFLX", "INTC",
        "CMCSA", "VZ", "NKE", "T", "PM", "MCD", "IBM", "QCOM", "HON", "AMD",
        "PYPL", "TXN", "LOW", "UNP", "UPS", "MS", "GS", "CAT", "NEE", "AMGN",
        "LMT", "MDT", "DE", "BA", "NOW", "BKNG", "RTX", "ISRG", "SPGI", "SCHW",
        "PLD", "TMO", "ELV", "C", "BDX", "SYK", "MO", "DUK", "SO", "BMY",
        "USB", "ADP", "GM", "CCI", "GILD", "NSC", "DHR", "HUM", "VRTX", "ZTS",
        "CSX", "FDX", "MMC", "PGR", "CL", "TJX", "REGN", "EQIX", "AON", "FIS",
        "APD", "ITW", "CI", "ECL", "MDLZ", "MCO", "ETN", "ROP", "ICE", "ILMN",
        "OXY", "D", "PNC", "CDNS", "HCA", "CHTR", "TGT", "PSA", "BIIB", "MAR",
        "EXC", "CME", "AZO", "CNC", "SHW", "F", "LHX", "TRV", "SRE", "GIS",
        "AEP", "DXCM", "WELL", "KMB", "WM", "ORLY", "TT", "IDXX", "PAYX", "MPC",
        "HES", "DOW", "STZ", "XEL", "MSCI", "KMI", "ALL", "LRCX", "A", "CMG",
        "EBAY", "HLT", "PCAR", "AIG", "YUM", "EOG", "KR", "TEL", "AFL", "VLO",
        "PPG", "AME", "WST", "FTNT", "SBAC", "CTAS", "PEG", "WEC", "VFC", "ED",
        "KEYS", "FANG", "MTD", "FAST", "RMD", "ABC", "DAL", "ANSS", "NUE", "DFS",
        "TSCO", "WMB", "GWW", "HIG", "CAG", "BAX", "OTIS", "ODFL", "IFF", "BALL",
        "ZBH", "DHI", "CLX", "CDW", "DTE", "RSG", "VMC", "GLW", "LKQ", "FRC"
    ]


    # Save stock symbols to file
    with open(STOCKS_FILE, "w") as f:
        for stock in top_200_stocks:
            f.write(stock + "\n")

    print(f"Stock symbols saved to {STOCKS_FILE}")

    # Read stock symbols from file
    with open(STOCKS_FILE, "r") as f:
        stock_symbols = [line.strip() for line in f.readlines()]
        


    # Function to fetch stock data from API
    def fetch_stock_data(symbol):
        url = f"https://www.alphavantage.co/query?function=GLOBAL_QUOTE&symbol={symbol}&apikey={ALPHA_VANTAGE_API_KEY}"
        try:
            response = requests.get(url)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            print(f"Error fetching data for {symbol}: {e}")
            return None

    # Load existing JSON data if the file exists
    try:
        with open(OUTPUT_JSON, "r") as f:
            all_data = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        all_data = {}

    # Fetch data for each stock and save immediately after each API call
    for stock in stock_symbols:
        if stock in all_data:
            print(f"Skipping {stock}, already fetched.")
            continue  # Avoid duplicate API calls

        print(f"Fetching data for {stock}...")
        data = fetch_stock_data(stock)

        if data:
            all_data[stock] = data  # Store the stock data

            # Save data to a temporary file before renaming
            with open(TEMP_JSON, "w") as f:
                json.dump(all_data, f, indent=4)

            # Rename temp file to actual JSON file (prevents corruption if script crashes)
            os.replace(TEMP_JSON, OUTPUT_JSON)

        time.sleep(12)  # Respect AlphaVantage rate limits (5 requests/minute)

    print(f"Stock data saved to {OUTPUT_JSON}")