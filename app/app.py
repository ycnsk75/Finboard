import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import numpy as np
from datetime import datetime, timedelta
from google.cloud import bigquery
from google.oauth2 import service_account
import os
import time

# Configuration de la page
st.set_page_config(
    page_title="Green Investment Dashboard",
    page_icon="🌿",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# Styles CSS personnalisés
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem !important;
        font-weight: 700 !important;
        color: #1E8449 !important;
        margin-bottom: 0.2rem !important;
        text-align: center;
    }
    .sub-header {
        font-size: 1.8rem !important;
        font-weight: 600 !important;
        color: #1E8449 !important;
        margin-top: 2rem !important;
        margin-bottom: 1rem !important;
        padding-left: 0.5rem;
        border-left: 4px solid #1E8449;
    }
    .card-container {
        background-color: #F8F9FA;
        border-radius: 10px;
        padding: 1rem;
        box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
        margin-bottom: 1rem;
    }
    .metric-card {
        background-color: white;
        border-radius: 8px;
        padding: 1rem;
        box-shadow: 0 2px 4px rgba(0, 0, 0, 0.05);
        border-top: 4px solid #1E8449;
        height: 100%;
        transition: transform 0.3s ease;
    }
    .metric-card:hover {
        transform: translateY(-5px);
        box-shadow: 0 4px 8px rgba(0, 0, 0, 0.1);
    }
    .chart-container {
        background-color: white;
        border-radius: 8px;
        padding: 1rem;
        box-shadow: 0 2px 4px rgba(0, 0, 0, 0.05);
        margin-top: 1rem;
        margin-bottom: 1.5rem;
    }
    .filter-section {
        background-color: white;
        border-radius: 8px;
        padding: 1rem;
        box-shadow: 0 2px 4px rgba(0, 0, 0, 0.05);
        margin-bottom: 1rem;
    }
    .footer {
        text-align: center;
        margin-top: 3rem;
        padding: 1rem;
        background-color: #F8F9FA;
        border-radius: 8px;
    }
    .small-text {
        font-size: 0.8rem;
        color: #6c757d;
    }
    .company-name {
        font-weight: 600;
        color: #2C3E50;
        margin-bottom: 0.2rem;
    }
    .ticker {
        font-size: 0.8rem;
        color: #6c757d;
        margin-bottom: 0.5rem;
    }
    .esg-score {
        font-size: 1.2rem;
        font-weight: 700;
        color: #1E8449;
        margin-bottom: 0.2rem;
    }
    .focus-tag {
        display: inline-block;
        background-color: rgba(30, 132, 73, 0.2);
        color: #1E8449;
        padding: 0.2rem 0.5rem;
        border-radius: 4px;
        font-size: 0.7rem;
        font-weight: 600;
        margin-bottom: 0.5rem;
    }
    .region-exchange {
        font-size: 0.7rem;
        color: #6c757d;
    }
    .gradient-divider {
        width: 100%;
        height: 4px;
        background: linear-gradient(90deg, #1E8449, #82E0AA, #D5F5E3);
        margin: 2rem 0;
        border-radius: 2px;
    }
    .refresh-button {
        background-color: #1E8449;
        color: white;
        border-radius: 5px;
        padding: 0.5rem 1rem;
        font-weight: 600;
    }
    .refresh-button:disabled {
        background-color: #6c757d;
        cursor: not-allowed;
    }
</style>
""", unsafe_allow_html=True)

# BigQuery Client Setup for Cloud Run
def get_bigquery_client():
    key_path = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
    if not key_path:
        raise ValueError("GOOGLE_APPLICATION_CREDENTIALS environment variable not set")
    credentials = service_account.Credentials.from_service_account_file(key_path)
    client = bigquery.Client(credentials=credentials, project=credentials.project_id)
    return client

# Load current data from BigQuery
@st.cache_data
def load_stock_data():
    client = get_bigquery_client()
    query = """
    SELECT name AS Company, ticker AS Ticker, green_efficiency_score AS "Green Score", 
           type AS Category, current_value AS "Current Price", marketcap_usd AS Marketcap
    FROM `devops-practice-449210.finboard.green_investments`
    WHERE asset_type = 'stock'
    """
    df = client.query(query).to_dataframe()
    return df

@st.cache_data
def load_crypto_data():
    client = get_bigquery_client()
    query = """
    SELECT name AS Coin, type AS Type, marketcap_usd AS Marketcap, 
           current_value AS "Current Price", co2_emissions_weekly_kg * 52 / 1000 AS "CO₂ Emissions (annualised)"
    FROM `devops-practice-449210.finboard.green_investments`
    WHERE asset_type = 'crypto'
    """
    df = client.query(query).to_dataframe()
    df["Marketcap"] = df["Marketcap"].apply(lambda x: f"{x/1e9:.2f}B" if x >= 1e9 else f"{x/1e6:.2f}M")
    df["CO₂ Emissions (annualised)"] = df["CO₂ Emissions (annualised)"].apply(
        lambda x: f"{x:.2f} tonnes" if x < 1000 else f"{x/1000:.1f}k tonnes"
    )
    return df

# Fetch historical data from BigQuery raw tables
@st.cache_data
def generate_historical_data(ticker, days=365, is_crypto=False):
    client = get_bigquery_client()
    table_name = "raw_coincap_prices" if is_crypto else "raw_finnhub_financials"
    identifier = "coin" if is_crypto else "ticker"
    price_col = "price_usd" if is_crypto else "current_price"
    
    start_date = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
    
    query = f"""
    SELECT 
        REGEXP_EXTRACT(_FILE_NAME, r'\\d{{4}}-\\d{{2}}-\\d{{2}}') AS date,
        {identifier}, 
        {price_col} AS Price
    FROM `devops-practice-449210.finboard.{table_name}`
    WHERE {identifier} = @identifier
    AND REGEXP_EXTRACT(_FILE_NAME, r'\\d{{4}}-\\d{{2}}-\\d{{2}}') >= @start_date
    ORDER BY date ASC
    """
    
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("identifier", "STRING", ticker),
            bigquery.ScalarQueryParameter("start_date", "STRING", start_date)
        ]
    )
    
    df = client.query(query, job_config=job_config).to_dataframe()
    
    if not df.empty:
        df['Date'] = pd.to_datetime(df['date'])
        df['Volume'] = np.random.uniform(50000, 5000000, size=len(df)).astype(int)
        df = df[['Date', 'Price', 'Volume']]
    
    if df.empty:
        return pd.DataFrame({'Date': [], 'Price': [], 'Volume': []})
    
    return df

# Fonction pour attribuer des couleurs basées sur le score vert
def get_green_color(score):
    if pd.isna(score):
        return "#6c757d"  # Gris pour NULL
    elif score > 5000000:
        return "#1E8449"  # Vert foncé
    elif score >= 500000:
        return "#27AE60"  # Vert moyen
    else:
        return "#E74C3C"  # Rouge

# Fonction pour créer une étiquette formattée pour les métriques
def format_metric_label(label, value, unit=""):
    return f"<span style='font-size:0.8rem;color:#6c757d;'>{label}</span><br><span style='font-size:1.2rem;font-weight:600;'>{value}{unit}</span>"

# Initialize session state for refresh cooldown
if 'last_refresh' not in st.session_state:
    st.session_state.last_refresh = None

# Refresh button logic
def refresh_data():
    current_time = datetime.now()
    if st.session_state.last_refresh is None or (current_time - st.session_state.last_refresh).total_seconds() >= 3600:
        # Clear cache and reload data
        st.cache_data.clear()
        st.session_state.last_refresh = current_time
        st.success("Données rafraîchies avec succès !")
        # Force rerun to reflect new data
        st.rerun()
    else:
        remaining_seconds = 3600 - (current_time - st.session_state.last_refresh).total_seconds()
        minutes_left = int(remaining_seconds // 60)
        seconds_left = int(remaining_seconds % 60)
        st.warning(f"Vous devez attendre encore {minutes_left} minutes et {seconds_left} secondes avant de pouvoir rafraîchir à nouveau.")

# Add refresh button below title
st.markdown("<p class='main-header'>🌿 Green Investment Dashboard</p>", unsafe_allow_html=True)
st.markdown("<p style='text-align:center;color:#6c757d;margin-bottom:1rem;'>Analyse des investissements durables et écologiques</p>", unsafe_allow_html=True)
col1, col2, col3 = st.columns([4, 1, 4])
with col2:
    st.button("Rafraîchir les données", on_click=refresh_data, key="refresh_button", help="Cliquez pour mettre à jour les données", disabled=(st.session_state.last_refresh is not None and (datetime.now() - st.session_state.last_refresh).total_seconds() < 3600))

# Chargement des données actuelles depuis BigQuery
stocks_df = load_stock_data()
crypto_df = load_crypto_data()

# Tabs pour passer entre stocks et crypto
tab1, tab2 = st.tabs(["🏢 Green Stocks", "💰 Green Cryptocurrencies"])

# Section 1: Green Stocks
with tab1:
    st.markdown("<p class='sub-header'>Green Stocks</p>", unsafe_allow_html=True)
    
    with st.container():
        st.markdown("<div class='filter-section'>", unsafe_allow_html=True)
        col1, col2, col3 = st.columns(3)
        with col1:
            category_filter = st.multiselect("Filtrer par catégorie", 
                                            options=stocks_df["Category"].unique(), 
                                            default=stocks_df["Category"].unique(),
                                            key="category_filter")
        with col2:
            price_min = st.slider("Prix minimum (USD)", 
                                 min_value=0.0, 
                                 max_value=float(stocks_df["Current Price"].max() or 1000), 
                                 value=0.0,
                                 key="price_slider")
        with col3:
            green_min = st.slider("Score vert minimum", 
                                 min_value=0, 
                                 max_value=int(stocks_df["Green Score"].max() or 10000000), 
                                 value=0,
                                 key="green_slider")
        st.markdown("</div>", unsafe_allow_html=True)
    
    filtered_stocks = stocks_df[
        (stocks_df["Category"].isin(category_filter)) &
        (stocks_df["Current Price"] >= price_min) &
        (stocks_df["Green Score"] >= green_min)
    ]
    
    if not filtered_stocks.empty:
        st.markdown("<p style='font-size:1.2rem;font-weight:600;margin-top:1rem;'>Key Metrics</p>", unsafe_allow_html=True)
        
        num_stocks = len(filtered_stocks)
        num_cols = min(4, num_stocks)
        num_rows = (num_stocks + num_cols - 1) // num_cols
        
        for row in range(num_rows):
            cols = st.columns(num_cols)
            for i in range(num_cols):
                idx = row * num_cols + i
                if idx < num_stocks:
                    stock = filtered_stocks.iloc[idx]
                    with cols[i]:
                        score_display = f"{stock['Green Score']:,.0f}" if pd.notna(stock['Green Score']) else "N/A"
                        color = get_green_color(stock['Green Score'])
                        st.markdown(f"""
                        <div class='metric-card'>
                            <p class='company-name'>{stock['Company']}</p>
                            <p class='ticker'>{stock['Ticker']}</p>
                            <p class='esg-score' style='color:{color};'>Green Score: {score_display}</p>
                            <p class='focus-tag'>{stock['Category']}</p>
                            <p class='region-exchange'>Price: ${stock['Current Price']:.2f}</p>
                        </div>
                        """, unsafe_allow_html=True)
        
        st.markdown("<div class='chart-container'>", unsafe_allow_html=True)
        col1, col2 = st.columns([3, 1])
        with col1:
            selected_stock = st.selectbox(
                "Sélectionnez une action pour voir les données historiques", 
                filtered_stocks["Ticker"] + " - " + filtered_stocks["Company"],
                key="stock_selector"
            )
        with col2:
            time_range = st.radio(
                "Période",
                ["1M", "3M", "6M", "1A", "Tout"],
                horizontal=True,
                key="stock_time_range"
            )
        
        if time_range == "1M":
            days = 30
        elif time_range == "3M":
            days = 90
        elif time_range == "6M":
            days = 180
        elif time_range == "1A":
            days = 365
        else:
            days = 365
        
        selected_ticker = selected_stock.split(" - ")[0]
        selected_company = selected_stock.split(" - ")[1]
        
        historical_stock_data = generate_historical_data(selected_ticker, days=days, is_crypto=False)
        
        if historical_stock_data.empty:
            st.warning(f"Aucune donnée historique disponible pour {selected_ticker} dans BigQuery pour la période sélectionnée.")
        else:
            line_color = "#2C3E50"  # Default color since Pattern isn’t available
            
            fig = go.Figure()
            fig.add_trace(go.Scatter(
                x=historical_stock_data['Date'],
                y=historical_stock_data['Price'],
                mode='lines',
                name=selected_ticker,
                line=dict(color=line_color, width=2.5),
                hovertemplate='Date: %{x}<br>Price: $%{y:.2f}<extra></extra>'
            ))
            
            historical_stock_data['MA20'] = historical_stock_data['Price'].rolling(window=20, min_periods=1).mean()
            historical_stock_data['MA50'] = historical_stock_data['Price'].rolling(window=50, min_periods=1).mean()
            
            fig.add_trace(go.Scatter(
                x=historical_stock_data['Date'],
                y=historical_stock_data['MA20'],
                mode='lines',
                name='MA20',
                line=dict(color='rgba(255, 165, 0, 0.7)', width=1.5, dash='dot'),
                hovertemplate='MA20: $%{y:.2f}<extra></extra>'
            ))
            
            fig.add_trace(go.Scatter(
                x=historical_stock_data['Date'],
                y=historical_stock_data['MA50'],
                mode='lines',
                name='MA50',
                line=dict(color='rgba(128, 0, 128, 0.7)', width=1.5, dash='dash'),
                hovertemplate='MA50: $%{y:.2f}<extra></extra>'
            ))
            
            if len(historical_stock_data) > 20:
                annotation_points = sorted(np.random.choice(range(1, len(historical_stock_data) - 1), min(3, len(historical_stock_data) - 2), replace=False))
                events = ["Annonce verte", "Expansion", "Nouveau projet"]
                for i, point in enumerate(annotation_points):
                    fig.add_annotation(
                        x=historical_stock_data.iloc[point]['Date'],
                        y=historical_stock_data.iloc[point]['Price'],
                        text=events[i],
                        showarrow=True,
                        arrowhead=2,
                        arrowsize=1,
                        arrowwidth=1,
                        arrowcolor="#636363",
                        ax=0,
                        ay=-40
                    )
            
            fig.update_layout(
                title=f"{selected_company} ({selected_ticker}) - Évolution du cours",
                xaxis_title="Date",
                yaxis_title="Prix (USD)",
                height=500,
                template="plotly_white",
                hovermode="x unified",
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
                xaxis=dict(
                    rangeselector=dict(
                        buttons=list([
                            dict(count=1, label="1m", step="month", stepmode="backward"),
                            dict(count=6, label="6m", step="month", stepmode="backward"),
                            dict(count=1, label="YTD", step="year", stepmode="todate"),
                            dict(count=1, label="1y", step="year", stepmode="backward"),
                            dict(step="all")
                        ]),
                        bgcolor="#F9F9F9",
                        activecolor="#1E8449"
                    ),
                    rangeslider=dict(visible=True),
                    type="date"
                )
            )
            
            min_price = historical_stock_data['Price'].min()
            max_price = historical_stock_data['Price'].max()
            
            fig.add_shape(type="rect", xref="paper", yref="y", x0=0, y0=min_price, x1=1, y1=min_price * 1.1, fillcolor="rgba(242, 242, 242, 0.5)", line_width=0)
            fig.add_shape(type="rect", xref="paper", yref="y", x0=0, y0=max_price * 0.9, x1=1, y1=max_price, fillcolor="rgba(242, 242, 242, 0.5)", line_width=0)
            
            st.plotly_chart(fig, use_container_width=True)
        
        st.markdown("</div>", unsafe_allow_html=True)
        
        chart_tabs = st.tabs(["Volume", "Comparaison", "Analyse"])
        
        with chart_tabs[0]:
            st.markdown("<div class='chart-container'>", unsafe_allow_html=True)
            if historical_stock_data.empty:
                st.warning("Pas de données de volume disponibles.")
            else:
                volume_colors = []
                for i in range(len(historical_stock_data)):
                    if i > 0 and historical_stock_data.iloc[i]['Price'] > historical_stock_data.iloc[i-1]['Price']:
                        volume_colors.append("rgba(39, 174, 96, 0.7)")
                    else:
                        volume_colors.append("rgba(231, 76, 60, 0.7)")
                
                volume_fig = go.Figure()
                volume_fig.add_trace(go.Bar(
                    x=historical_stock_data['Date'],
                    y=historical_stock_data['Volume'],
                    marker_color=volume_colors,
                    name="Volume",
                    hovertemplate='Date: %{x}<br>Volume: %{y:,.0f}<extra></extra>'
                ))
                
                volume_fig.update_layout(
                    title=f"Volume d'échanges pour {selected_ticker}",
                    xaxis_title="Date",
                    yaxis_title="Volume",
                    height=400,
                    template="plotly_white",
                    bargap=0.1,
                    yaxis=dict(type='log', title_font=dict(size=14))
                )
                st.plotly_chart(volume_fig, use_container_width=True)
            st.markdown("</div>", unsafe_allow_html=True)
        
        with chart_tabs[1]:
            st.markdown("<div class='chart-container'>", unsafe_allow_html=True)
            competitors = filtered_stocks[
                (filtered_stocks["Category"] == stocks_df[stocks_df["Ticker"] == selected_ticker]["Category"].values[0]) &
                (filtered_stocks["Ticker"] != selected_ticker)
            ].head(3)
            
            if not competitors.empty and not historical_stock_data.empty:
                comp_fig = go.Figure()
                base_price = historical_stock_data.iloc[0]['Price']
                normalized_prices = (historical_stock_data['Price'] / base_price) * 100
                
                comp_fig.add_trace(go.Scatter(
                    x=historical_stock_data['Date'],
                    y=normalized_prices,
                    mode='lines',
                    name=selected_ticker,
                    line=dict(color=line_color, width=2.5),
                    hovertemplate='Date: %{x}<br>Valeur relative: %{y:.1f}%<extra></extra>'
                ))
                
                colors = ["#3498DB", "#9B59B6", "#F1C40F"]
                for i, (_, comp) in enumerate(competitors.iterrows()):
                    comp_data = generate_historical_data(comp["Ticker"], days=days, is_crypto=False)
                    if not comp_data.empty:
                        comp_base = comp_data.iloc[0]['Price']
                        comp_normalized = (comp_data['Price'] / comp_base) * 100
                        
                        comp_fig.add_trace(go.Scatter(
                            x=comp_data['Date'],
                            y=comp_normalized,
                            mode='lines',
                            name=comp["Ticker"],
                            line=dict(color=colors[i], width=1.5),
                            hovertemplate='Date: %{x}<br>Valeur relative: %{y:.1f}%<extra></extra>'
                        ))
                
                comp_fig.update_layout(
                    title="Comparaison de performance relative (base 100)",
                    xaxis_title="Date",
                    yaxis_title="Performance (%)",
                    height=400,
                    template="plotly_white",
                    hovermode="x unified",
                    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
                )
                st.plotly_chart(comp_fig, use_container_width=True)
            else:
                st.info("Pas assez de données historiques pour effectuer une comparaison.")
            st.markdown("</div>", unsafe_allow_html=True)
            
        with chart_tabs[2]:
            st.markdown("<div class='chart-container'>", unsafe_allow_html=True)
            col1, col2 = st.columns(2)
            
            with col1:
                if historical_stock_data.empty:
                    st.warning("Pas de données historiques pour le résumé.")
                else:
                    current_price = historical_stock_data.iloc[-1]['Price']
                    start_price = historical_stock_data.iloc[0]['Price']
                    perf = ((current_price / start_price) - 1) * 100
                    perf_color = "green" if perf > 0 else "red"
                    
                    st.markdown(f"""
                    <h3 style='font-size:1.3rem;margin-bottom:1rem;'>Résumé de la performance</h3>
                    <div style='background-color:#F8F9FA;padding:1rem;border-radius:8px;'>
                        <p><b>Dernier prix:</b> ${current_price:.2f}</p>
                        <p><b>Performance sur la période:</b> <span style='color:{perf_color};'>{perf:.2f}%</span></p>
                        <p><b>Prix le plus bas:</b> ${historical_stock_data['Price'].min():.2f}</p>
                        <p><b>Prix le plus haut:</b> ${historical_stock_data['Price'].max():.2f}</p>
                        <p><b>Volume moyen:</b> {historical_stock_data['Volume'].mean():.0f}</p>
                        <p><b>Score vert:</b> {stocks_df[stocks_df['Ticker'] == selected_ticker]['Green Score'].values[0]:,.0f}</p>
                    </div>
                    """, unsafe_allow_html=True)
                
            with col2:
                if historical_stock_data.empty:
                    st.warning("Pas de données pour l'analyse.")
                else:
                    historical_stock_data['daily_return'] = historical_stock_data['Price'].pct_change() * 100
                    volatility = historical_stock_data['daily_return'].std()
                    
                    hist_fig = go.Figure()
                    hist_fig.add_trace(go.Histogram(
                        x=historical_stock_data['daily_return'].dropna(),
                        nbinsx=30,
                        marker_color='rgba(30, 132, 73, 0.6)',
                        marker_line_color='rgba(30, 132, 73, 1)',
                        marker_line_width=1
                    ))
                    
                    hist_fig.update_layout(
                        title="Distribution des rendements journaliers",
                        xaxis_title="Rendement (%)",
                        yaxis_title="Fréquence",
                        height=300,
                        template="plotly_white"
                    )
                    
                    st.plotly_chart(hist_fig, use_container_width=True)
                    st.markdown(f"<p><b>Volatilité (écart-type):</b> {volatility:.2f}%</p>", unsafe_allow_html=True)
            
            st.markdown("</div>", unsafe_allow_html=True)
    
    else:
        st.warning("Aucune action ne correspond aux filtres sélectionnés.")

# Section 2: Green Cryptocurrencies
with tab2:
    st.markdown("<p class='sub-header'>Green Cryptocurrencies</p>", unsafe_allow_html=True)
    
    with st.container():
        st.markdown("<div class='filter-section'>", unsafe_allow_html=True)
        col1, col2 = st.columns(2)
        with col1:
            type_filter = st.multiselect("Filtrer par type de consensus", 
                                       options=crypto_df["Type"].unique(), 
                                       default=crypto_df["Type"].unique(),
                                       key="crypto_type_filter")
        with col2:
            price_max = st.slider("Prix maximum (USD)", 
                              min_value=0.0, 
                              max_value=float(crypto_df["Current Price"].max() or 100000), 
                              value=float(crypto_df["Current Price"].max() or 100000),
                              step=0.01,
                              key="price_slider_crypto")
        st.markdown("</div>", unsafe_allow_html=True)
    
    filtered_cryptos = crypto_df[
        (crypto_df["Type"].isin(type_filter)) &
        (crypto_df["Current Price"] <= price_max)
    ]
    
    if not filtered_cryptos.empty:
        st.markdown("<p style='font-size:1.2rem;font-weight:600;margin-top:1rem;'>Cryptomonnaies écologiques</p>", unsafe_allow_html=True)
        
        num_cryptos = len(filtered_cryptos)
        num_cols = 3
        num_rows = (num_cryptos + num_cols - 1) // num_cols
        
        for row in range(num_rows):
            cols = st.columns(num_cols)
            for i in range(num_cols):
                idx = row * num_cols + i
                if idx < num_cryptos:
                    crypto = filtered_cryptos.iloc[idx]
                    with cols[i]:
                        st.markdown(f"""
                        <div class='metric-card'>
                            <p class='company-name'>{crypto['Coin']}</p>
                            <p class='ticker'>{crypto['Type']}</p>
                            <p class='focus-tag'>Marketcap: {crypto['Marketcap']}</p>
                            <p style='font-size:0.9rem;margin-top:0.5rem;'><b>Prix:</b> ${crypto['Current Price']:.2f}</p>
                            <p style='font-size:0.9rem;'><b>CO₂:</b> {crypto['CO₂ Emissions (annualised)']}</p>
                        </div>
                        """, unsafe_allow_html=True)
        
        st.markdown("<div class='chart-container'>", unsafe_allow_html=True)
        selected_crypto = st.selectbox(
            "Sélectionnez une cryptomonnaie pour voir les données historiques", 
            filtered_cryptos["Coin"],
            key="crypto_selector"
        )
        
        historical_crypto_data = generate_historical_data(selected_crypto, days=365, is_crypto=True)
        
        if historical_crypto_data.empty:
            st.warning(f"Aucune donnée historique disponible pour {selected_crypto} dans BigQuery pour la période sélectionnée.")
        else:
            crypto_color = "#2C3E50"  # Default since Pattern isn’t available
            
            crypto_fig = go.Figure()
            crypto_fig.add_trace(go.Scatter(
                x=historical_crypto_data['Date'],
                y=historical_crypto_data['Price'],
                mode='lines',
                name=selected_crypto,
                line=dict(color=crypto_color, width=2.5),
                hovertemplate='Date: %{x}<br>Prix: $%{y:.2f}<extra></extra>'
            ))
            
            crypto_fig.update_layout(
                title=f"{selected_crypto} - Évolution du cours",
                xaxis_title="Date",
                yaxis_title="Prix (USD)",
                height=500,
                template="plotly_white",
                xaxis=dict(
                    rangeselector=dict(
                        buttons=list([
                            dict(count=1, label="1m", step="month", stepmode="backward"),
                            dict(count=6, label="6m", step="month", stepmode="backward"),
                            dict(count=1, label="YTD", step="year", stepmode="todate"),
                            dict(count=1, label="1y", step="year", stepmode="backward"),
                            dict(step="all")
                        ]),
                        bgcolor="#F9F9F9",
                        activecolor="#1E8449"
                    ),
                    rangeslider=dict(visible=True),
                    type="date"
                )
            )
            
            st.plotly_chart(crypto_fig, use_container_width=True)
        st.markdown("</div>", unsafe_allow_html=True)
        
        st.markdown("<p class='sub-header'>Impact environnemental comparatif</p>", unsafe_allow_html=True)
        impact_fig = go.Figure()
        sorted_cryptos = filtered_cryptos.sort_values(by="CO₂ Emissions (annualised)", key=lambda x: x.str.extract(r'([\d.]+)').astype(float)[0])
        
        impact_fig.add_trace(go.Bar(
            x=sorted_cryptos["Coin"],
            y=sorted_cryptos["CO₂ Emissions (annualised)"].str.extract(r'([\d.]+)').astype(float)[0],
            marker_color="#27AE60",
            name="CO₂ Emissions (tonnes)",
            hovertemplate='Crypto: %{x}<br>CO₂: %{y:.2f} tonnes<extra></extra>'
        ))
        
        impact_fig.update_layout(
            title="Comparaison des émissions de CO₂ annuelles",
            xaxis_title="Cryptomonnaie",
            yaxis_title="CO₂ Emissions (tonnes)",
            height=400,
            template="plotly_white",
            yaxis=dict(type="log")
        )
        
        st.plotly_chart(impact_fig, use_container_width=True)
        
        st.markdown("<div class='chart-container'>", unsafe_allow_html=True)
        st.markdown("""
        <h3 style='font-size:1.3rem;margin-bottom:1rem;'>Pourquoi ces cryptomonnaies sont considérées comme écologiques</h3>
        <p>Ces cryptomonnaies utilisent des mécanismes de consensus alternatifs qui consomment moins d'énergie, reflété par leurs émissions de CO₂.</p>
        """, unsafe_allow_html=True)
        st.table(filtered_cryptos[["Coin", "Type", "CO₂ Emissions (annualised)"]])
        st.markdown("</div>", unsafe_allow_html=True)
    else:
        st.warning("Aucune cryptomonnaie ne correspond aux filtres sélectionnés.")

# Divider
st.markdown("<div class='gradient-divider'></div>", unsafe_allow_html=True)

# Footer avec disclaimer
st.markdown("""
<div class='footer'>
    <p style='font-weight:600;'>Green Investment Dashboard</p>
    <p class='small-text'>Powered by BigQuery data from Cloud Composer pipeline (gs://projectbigquery/), deployed on Cloud Run.</p>
</div>
""", unsafe_allow_html=True)