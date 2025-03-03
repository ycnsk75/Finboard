import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
from google.cloud import bigquery
from google.oauth2 import service_account
import os

# Page configuration
st.set_page_config(
    page_title="Green Investment Dashboard",
    page_icon="🌿",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# Custom CSS with adjustments for gauge integration
st.markdown("""
<style>
    .main-header {font-size: 2.5rem !important; font-weight: 700 !important; color: #1E8449 !important; margin-bottom: 0.2rem !important; text-align: center;}
    .sub-header {font-size: 1.8rem !important; font-weight: 600 !important; color: #1E8449 !important; margin-top: 2rem !important; margin-bottom: 1rem !important; padding-left: 0.5rem; border-left: 4px solid #1E8449;}
    .card-container {background-color: #F8F9FA; border-radius: 10px; padding: 1rem; box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1); margin-bottom: 1rem;}
    .metric-card {background-color: white; border-radius: 8px; padding: 1rem; box-shadow: 0 2px 4px rgba(0, 0, 0, 0.05); border-top: 4px solid #1E8449; height: 280px; transition: transform 0.3s ease; display: flex; flex-direction: column; justify-content: space-between;}
    .metric-card:hover {transform: translateY(-5px); box-shadow: 0 4px 8px rgba(0, 0, 0, 0.1);}
    .chart-container {background-color: white; border-radius: 8px; padding: 1rem; box-shadow: 0 2px 4px rgba(0, 0, 0, 0.05); margin-top: 1rem; margin-bottom: 1.5rem;}
    .filter-section {background-color: white; border-radius: 8px; padding: 1rem; box-shadow: 0 2px 4px rgba(0, 0, 0, 0.05); margin-bottom: 1rem;}
    .footer {text-align: center; margin-top: 3rem; padding: 1rem; background-color: #F8F9FA; border-radius: 8px;}
    .small-text {font-size: 0.8rem; color: #6c757d;}
    .company-name {font-weight: 600; color: #2C3E50; margin-bottom: 0.2rem;}
    .ticker {font-size: 0.8rem; color: #6c757d; margin-bottom: 0.5rem;}
    .focus-tag {display: inline-block; background-color: rgba(30, 132, 73, 0.2); color: #1E8449; padding: 0.2rem 0.5rem; border-radius: 4px; font-size: 0.7rem; font-weight: 600; margin-bottom: 0.5rem;}
    .region-exchange {font-size: 0.7rem; color: #6c757d;}
    .gradient-divider {width: 100%; height: 4px; background: linear-gradient(90deg, #1E8449, #82E0AA, #D5F5E3); margin: 2rem 0; border-radius: 2px;}
    .refresh-button {background-color: #1E8449; color: white; border-radius: 5px; padding: 0.5rem 1rem; font-weight: 600;}
    .refresh-button:disabled {background-color: #6c757d; cursor: not-allowed;}
</style>
""", unsafe_allow_html=True)

# BigQuery Client Setup
def get_bigquery_client():
    key_path = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
    if not key_path:
        raise ValueError("GOOGLE_APPLICATION_CREDENTIALS environment variable not set")
    credentials = service_account.Credentials.from_service_account_file(key_path)
    client = bigquery.Client(credentials=credentials, project=credentials.project_id)
    return client

# Load data from BigQuery
@st.cache_data
def load_green_investments():
    client = get_bigquery_client()
    query = """
    SELECT 
        asset_type,
        asset_name AS Name,
        asset_symbol AS Symbol,
        current_price AS "Current Price",
        market_cap AS Marketcap,
        co2_emissions_mt AS "CO2 Emissions (Mt)",
        energy_consumption AS "Energy Consumption (GW)",
        green_efficiency AS "Green Efficiency",
        environmental_score AS "Environmental Score",
        environmental_category AS "Environmental Category",
        asset_subtype AS "Subtype",
        estimated_esg AS "ESG Score",
        region AS Region,
        main_focus AS "Main Focus"
    FROM `devops-practice-449210.marts_green_finance.green_investment_overview`
    """
    df = client.query(query).to_dataframe()
    df["Marketcap"] = df["Marketcap"].apply(lambda x: f"{x/1e9:.2f}B" if x >= 1e9 else f"{x/1e6:.2f}M")
    return df

@st.cache_data
def load_historical_data(symbol, days=365, asset_type="Stock"):
    client = get_bigquery_client()
    table_name = "raw_coincap_prices" if asset_type == "Cryptocurrency" else "raw_finnhub_financials"
    identifier = "coin" if asset_type == "Cryptocurrency" else "ticker"
    price_col = "price_usd" if asset_type == "Cryptocurrency" else "current_price"
    
    start_date = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
    
    query = f"""
    SELECT 
        REGEXP_EXTRACT(_FILE_NAME, r'\\d{{4}}-\\d{{2}}-\\d{{2}}') AS date,
        {price_col} AS Price
    FROM `devops-practice-449210.finboard.{table_name}`
    WHERE {identifier} = @identifier
    AND REGEXP_EXTRACT(_FILE_NAME, r'\\d{{4}}-\\d{{2}}-\\d{{2}}') >= @start_date
    ORDER BY date ASC
    """
    
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("identifier", "STRING", symbol),
            bigquery.ScalarQueryParameter("start_date", "STRING", start_date)
        ]
    )
    
    df = client.query(query, job_config=job_config).to_dataframe()
    if not df.empty:
        df['Date'] = pd.to_datetime(df['date'])
    return df if not df.empty else pd.DataFrame({'Date': [], 'Price': []})

# Enhanced gauge function
def create_gauge(score):
    if pd.isna(score):
        score = 0
        bar_color = "#6c757d"
    else:
        score = float(score)
        if score <= 25:
            bar_color = f"rgba({int(139 + (score / 25) * (255 - 139))}, 0, 0, 1)"
        elif score <= 50:
            bar_color = f"rgba(255, {int(69 + ((score - 25) / 25) * (165 - 69))}, 0, 1)"
        elif score <= 75:
            bar_color = "#FFD700"
        else:
            bar_color = f"rgba(0, {int(255 - ((score - 75) / 25) * (255 - 139))}, 0, 1)"

    fig = go.Figure(go.Indicator(
        mode="gauge+number",
        value=score,
        domain={'x': [0, 1], 'y': [0, 1]},
        title={'text': "Env Score", 'font': {'size': 12}},
        gauge={
            'axis': {'range': [0, 100], 'tickwidth': 1, 'tickcolor': "black", 'ticklen': 5},
            'bar': {'color': bar_color, 'thickness': 0.3},
            'bgcolor': "white",
            'borderwidth': 2,
            'bordercolor': "#2C3E50",
            'steps': [
                {'range': [0, 12.5], 'color': "rgba(139, 0, 0, 0.3)"},
                {'range': [12.5, 25], 'color': "rgba(255, 0, 0, 0.3)"},
                {'range': [25, 37.5], 'color': "rgba(255, 69, 0, 0.3)"},
                {'range': [37.5, 50], 'color': "rgba(255, 165, 0, 0.3)"},
                {'range': [50, 75], 'color': "rgba(255, 215, 0, 0.3)"},
                {'range': [75, 87.5], 'color': "rgba(0, 255, 0, 0.3)"},
                {'range': [87.5, 100], 'color': "rgba(0, 139, 0, 0.3)"}
            ],
            'threshold': {
                'line': {'color': bar_color, 'width': 4},
                'thickness': 0.75,
                'value': score
            }
        },
        number={'font': {'size': 16, 'color': bar_color}}
    ))
    fig.update_layout(
        height=140,
        margin=dict(l=10, r=10, t=20, b=10),
        paper_bgcolor="rgba(0,0,0,0)",
        font={'color': "#2C3E50"}
    )
    return fig

# Refresh logic
if 'last_refresh' not in st.session_state:
    st.session_state.last_refresh = None

def refresh_data():
    current_time = datetime.now()
    if st.session_state.last_refresh is None or (current_time - st.session_state.last_refresh).total_seconds() >= 3600:
        st.cache_data.clear()
        st.session_state.last_refresh = current_time
        st.success("Data refreshed successfully!")
        st.rerun()
    else:
        remaining = 3600 - (current_time - st.session_state.last_refresh).total_seconds()
        minutes_left = int(remaining // 60)
        seconds_left = int(remaining % 60)
        st.warning(f"Please wait {minutes_left} minutes and {seconds_left} seconds before refreshing again.")

# Header and refresh button
st.markdown("<p class='main-header'>🌿 Green Investment Dashboard</p>", unsafe_allow_html=True)
st.markdown("<p style='text-align:center;color:#6c757d;margin-bottom:1rem;'>Sustainable Investment Insights</p>", unsafe_allow_html=True)
col1, col2, col3 = st.columns([4, 1, 4])
with col2:
    st.button("Refresh Data", on_click=refresh_data, key="refresh_button", 
              disabled=(st.session_state.last_refresh is not None and (datetime.now() - st.session_state.last_refresh).total_seconds() < 3600))

# Load data
df = load_green_investments()
stocks_df = df[df['asset_type'] == 'Stock'].copy()
crypto_df = df[df['asset_type'] == 'Cryptocurrency'].copy()

# Prepare full list of assets for searching
all_assets = df[["Symbol", "Name", "asset_type"]].copy()
all_assets["display"] = all_assets["Symbol"] + " - " + all_assets["Name"]

# Tabs
tab1, tab2 = st.tabs(["🏢 Green Stocks", "💰 Green Cryptocurrencies"])

# Green Stocks Tab
with tab1:
    st.markdown("<p class='sub-header'>Green Stocks</p>", unsafe_allow_html=True)
    
    # Filters
    with st.container():
        st.markdown("<div class='filter-section'>", unsafe_allow_html=True)
        col1, col2, col3 = st.columns(3)
        with col1:
            subtype_filter = st.multiselect("Filter by Category", options=stocks_df["Subtype"].unique(), default=stocks_df["Subtype"].unique())
        with col2:
            price_min = st.slider("Minimum Price (USD)", min_value=0.0, max_value=float(stocks_df["Current Price"].max() or 1000), value=0.0)
        with col3:
            env_score_min = st.slider("Minimum Environmental Score", min_value=0, max_value=100, value=0)
        st.markdown("</div>", unsafe_allow_html=True)
    
    filtered_stocks = stocks_df[
        (stocks_df["Subtype"].isin(subtype_filter)) &
        (stocks_df["Current Price"] >= price_min) &
        (stocks_df["Environmental Score"] >= env_score_min)
    ]
    
    if not filtered_stocks.empty:
        # Metrics Cards with Gauges
        st.markdown("<p style='font-size:1.2rem;font-weight:600;margin-top:1rem;'>Key Investments</p>", unsafe_allow_html=True)
        num_stocks = len(filtered_stocks)
        cols_per_row = 4
        num_rows = (num_stocks + cols_per_row - 1) // cols_per_row
        
        for row in range(num_rows):
            cols = st.columns(cols_per_row)
            for col_idx, idx in enumerate(range(row * cols_per_row, min((row + 1) * cols_per_row, num_stocks))):
                stock = filtered_stocks.iloc[idx]
                with cols[col_idx]:
                    st.markdown(f"""
                    <div class='metric-card'>
                        <div>
                            <p class='company-name'>{stock['Name']}</p>
                            <p class='ticker'>{stock['Symbol']}</p>
                            <p class='focus-tag'>{stock['Subtype']}</p>
                            <p class='region-exchange'>Price: ${stock['Current Price']:.2f}</p>
                            <p class='region-exchange'>CO₂: {stock['CO2 Emissions (Mt)']:.2f} Mt</p>
                        </div>
                    </div>
                    """, unsafe_allow_html=True)
                    gauge_fig = create_gauge(stock['Environmental Score'])
                    st.plotly_chart(gauge_fig, use_container_width=True, config={'displayModeBar': False})
        
        # Historical Chart with Searchable Dropdown
        st.markdown("<div class='chart-container'>", unsafe_allow_html=True)
        col1, col2 = st.columns([3, 1])
        with col1:
            stock_options = all_assets[all_assets["asset_type"] == "Stock"]["display"].tolist()
            selected_stock = st.selectbox("Search or select a stock for historical data", stock_options, 
                                          help="Type to search for any stock")
        with col2:
            time_range = st.radio("Time Range", ["1M", "3M", "6M", "1Y"], horizontal=True)
        
        days = {"1M": 30, "3M": 90, "6M": 180, "1Y": 365}[time_range]
        selected_symbol = selected_stock.split(" - ")[0]
        historical_data = load_historical_data(selected_symbol, days=days, asset_type="Stock")
        
        if historical_data.empty:
            st.warning(f"No historical data available for {selected_symbol}.")
        else:
            fig = go.Figure()
            fig.add_trace(go.Scatter(x=historical_data['Date'], y=historical_data['Price'], mode='lines', 
                                    name=selected_symbol, line=dict(color="#1E8449", width=2)))
            fig.update_layout(
                title=f"{selected_stock} - Price Trend",
                xaxis_title="Date",
                yaxis_title="Price (USD)",
                height=500,
                template="plotly_white",
                xaxis=dict(rangeslider=dict(visible=True))
            )
            st.plotly_chart(fig, use_container_width=True)
        st.markdown("</div>", unsafe_allow_html=True)
    else:
        st.warning("No stocks match the selected filters.")

# Green Cryptocurrencies Tab
with tab2:
    st.markdown("<p class='sub-header'>Green Cryptocurrencies</p>", unsafe_allow_html=True)
    
    # Filters
    with st.container():
        st.markdown("<div class='filter-section'>", unsafe_allow_html=True)
        col1, col2 = st.columns(2)
        with col1:
            subtype_filter = st.multiselect("Filter by Subtype", options=crypto_df["Subtype"].unique(), default=crypto_df["Subtype"].unique())
        with col2:
            price_max = st.slider("Maximum Price (USD)", min_value=0.0, max_value=float(crypto_df["Current Price"].max() or 100000), 
                                 value=float(crypto_df["Current Price"].max() or 100000))
        st.markdown("</div>", unsafe_allow_html=True)
    
    filtered_cryptos = crypto_df[
        (crypto_df["Subtype"].isin(subtype_filter)) &
        (crypto_df["Current Price"] <= price_max)
    ]
    
    if not filtered_cryptos.empty:
        # Metrics Cards with Gauges
        st.markdown("<p style='font-size:1.2rem;font-weight:600;margin-top:1rem;'>Key Cryptocurrencies</p>", unsafe_allow_html=True)
        num_cryptos = len(filtered_cryptos)
        cols_per_row = 3
        num_rows = (num_cryptos + cols_per_row - 1) // cols_per_row
        
        for row in range(num_rows):
            cols = st.columns(cols_per_row)
            for col_idx, idx in enumerate(range(row * cols_per_row, min((row + 1) * cols_per_row, num_cryptos))):
                crypto = filtered_cryptos.iloc[idx]
                with cols[col_idx]:
                    st.markdown(f"""
                    <div class='metric-card'>
                        <div>
                            <p class='company-name'>{crypto['Name']}</p>
                            <p class='ticker'>{crypto['Symbol']}</p>
                            <p class='focus-tag'>{crypto['Subtype']}</p>
                            <p class='region-exchange'>Price: ${crypto['Current Price']:.2f}</p>
                            <p class='region-exchange'>CO₂: {crypto['CO2 Emissions (Mt)']:.2f} Mt</p>
                        </div>
                    </div>
                    """, unsafe_allow_html=True)
                    gauge_fig = create_gauge(crypto['Environmental Score'])
                    st.plotly_chart(gauge_fig, use_container_width=True, config={'displayModeBar': False})
        
        # Historical Chart with Searchable Dropdown
        st.markdown("<div class='chart-container'>", unsafe_allow_html=True)
        crypto_options = all_assets[all_assets["asset_type"] == "Cryptocurrency"]["display"].tolist()
        selected_crypto = st.selectbox("Search or select a cryptocurrency for historical data", crypto_options, 
                                       help="Type to search for any cryptocurrency")
        selected_symbol = selected_crypto.split(" - ")[0]
        historical_data = load_historical_data(selected_symbol, days=365, asset_type="Cryptocurrency")
        
        if historical_data.empty:
            st.warning(f"No historical data available for {selected_symbol}.")
        else:
            fig = go.Figure()
            fig.add_trace(go.Scatter(x=historical_data['Date'], y=historical_data['Price'], mode='lines', 
                                    name=selected_symbol, line=dict(color="#1E8449", width=2)))
            fig.update_layout(
                title=f"{selected_crypto} - Price Trend",
                xaxis_title="Date",
                yaxis_title="Price (USD)",
                height=500,
                template="plotly_white",
                xaxis=dict(rangeslider=dict(visible=True))
            )
            st.plotly_chart(fig, use_container_width=True)
        st.markdown("</div>", unsafe_allow_html=True)
        
        # Environmental Impact Comparison
        st.markdown("<p class='sub-header'>Environmental Impact</p>", unsafe_allow_html=True)
        fig = px.bar(filtered_cryptos, x="Name", y="CO2 Emissions (Mt)", color="Environmental Score", 
                     title="CO2 Emissions by Cryptocurrency", height=400)
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.warning("No cryptocurrencies match the selected filters.")

# Footer
st.markdown("<div class='gradient-divider'></div>", unsafe_allow_html=True)
st.markdown("""
<div class='footer'>
    <p style='font-weight:600;'>Green Investment Dashboard</p>
    <p class='small-text'>Powered by BigQuery and dbt transformations. Data updated as of March 03, 2025.</p>
</div>
""", unsafe_allow_html=True)