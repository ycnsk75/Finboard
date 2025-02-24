import requests
from sklearn.pipeline import Pipeline
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import FunctionTransformer
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

API_KEY = ${API_KEY}
HEADERS = {
    "Authorization": f"Bearer {API_KEY}",
    "Accept": "application/json"
}

def fetch_ratings():
    url = "https://api.carbon-ratings.com/v2/ratings"
    response = requests.get(url, headers=HEADERS)
    return response.json()

ratings_data = fetch_ratings()
# Sample output: 
# [{"rating_id": 1, "entity_id": "BTC", "carbon_score": 45, ...}, ...]

def fetch_entities():
    url = "https://api.carbon-ratings.com/v2/entities"
    response = requests.get(url, headers=HEADERS)
    return response.json()

entities_data = fetch_entities()
# Sample output: 
# [{"entity_id": "BTC", "name": "Bitcoin", "industry": "Crypto Mining", ...}, ...]

def fetch_historical(entity_id):
    url = f"https://api.carbon-ratings.com/v2/historical/{entity_id}"
    response = requests.get(url, headers=HEADERS)
    return response.json()

btc_historical = fetch_historical("BTC")
# Sample output: 
# [{"timestamp": "2023-01-01", "carbon_score": 50, ...}, ...]


# Convert to DataFrames
df_ratings = pd.DataFrame(ratings_data)
df_entities = pd.DataFrame(entities_data)

# Merge on entity_id
merged_df = pd.merge(df_ratings, df_entities, on="entity_id", how="left")

merged_df.dropna(subset=["carbon_score"], inplace=True)  # Drop rows with missing scores
merged_df["energy_sources"].fillna("unknown", inplace=True)  # Fill missing energy sources

# Split energy_sources into renewable/fossil columns
merged_df["renewable_percent"] = merged_df["energy_sources"].apply(
    lambda x: x.get("renewable_percent") if isinstance(x, dict) else 0
)
merged_df["fossil_percent"] = merged_df["energy_sources"].apply(
    lambda x: x.get("fossil_fuel_percent") if isinstance(x, dict) else 0
)

# Filter for crypto/financial entities
filtered_df = merged_df[
    merged_df["industry"].isin(["Crypto Mining", "Banking", "Renewable Energy"])
]
# Sort by carbon_score (ascending = greener)
filtered_df = filtered_df.sort_values("carbon_score", ascending=True)

avg_scores = filtered_df.groupby("industry")["carbon_score"].mean()
# Output: Crypto Mining (55), Banking (30), Renewable Energy (10)

# Plot Bitcoin's carbon score over time
btc_history = pd.DataFrame(btc_historical)
plt.plot(btc_history["timestamp"], btc_history["carbon_score"])
plt.title("Bitcoin Carbon Score Trend")
plt.xlabel("Date")
plt.ylabel("Carbon Score")
plt.show()


sns.scatterplot(
    data=filtered_df, 
    x="renewable_percent", 
    y="carbon_score", 
    hue="industry"
)
plt.title("Renewable Energy vs. Carbon Score")




def preprocess_energy_sources(df):
    df["renewable_percent"] = df["energy_sources"].apply(
        lambda x: x.get("renewable_percent") if isinstance(x, dict) else 0
    )
    return df.drop("energy_sources", axis=1)

pipeline = Pipeline([
    ("merge_data", FunctionTransformer(lambda df: pd.merge(df, df_entities, on="entity_id"))),
    ("add_energy_columns", FunctionTransformer(preprocess_energy_sources)),
    ("impute_missing", SimpleImputer(strategy="mean"))  # For numeric columns
])

cleaned_data = pipeline.fit_transform(df_ratings)