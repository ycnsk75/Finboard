from google.cloud import bigquery
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import numpy as np
from datetime import datetime, timedelta
import random

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
</style>
""", unsafe_allow_html=True)

# Fonction pour générer des données historiques plus réalistes
def generate_historical_data(ticker, days=365, pattern="random"):
    np.random.seed(hash(ticker) % 10000)  # Assurer la cohérence pour un même ticker
    dates = [(datetime.now() - timedelta(days=i)).strftime('%Y-%m-%d') for i in range(days)]
    dates.reverse()  # Mettre les dates dans l'ordre chronologique
    
    base_price = np.random.uniform(10, 1000)
    volatility = np.random.uniform(0.01, 0.05)
    
    # Différents patterns de prix
    if pattern == "uptrend":
        trend_factor = np.linspace(0, 0.3, days)  # Tendance haussière
    elif pattern == "downtrend":
        trend_factor = np.linspace(0, -0.2, days)  # Tendance baissière
    elif pattern == "volatile":
        trend_factor = np.zeros(days)
        volatility *= 2  # Plus volatile
    elif pattern == "cyclical":
        # Tendance cyclique
        trend_factor = 0.2 * np.sin(np.linspace(0, 3 * np.pi, days))
    else:  # random
        trend_factor = np.zeros(days)
    
    # Simuler un mouvement de prix
    changes = np.random.normal(0.0005, volatility, days) + trend_factor
    price = base_price
    prices = [price]
    
    for change in changes:
        price = price * (1 + change)
        prices.append(price)
    
    # Assurer que les prix restent positifs
    prices = [max(0.1, p) for p in prices][1:]
    
    # Volume: plus de variations pour plus de réalisme
    volume_base = np.random.uniform(50000, 5000000)
    volumes = []
    
    for i in range(days):
        # Volume plus élevé lors de grands mouvements de prix
        price_change = abs(changes[i])
        volume_factor = 1 + 5 * price_change if price_change > volatility else 1
        daily_volume = int(volume_base * np.random.uniform(0.7, 1.3) * volume_factor)
        volumes.append(daily_volume)
    
    df = pd.DataFrame({
        'Date': dates,
        'Price': prices,
        'Volume': volumes
    })
    df['Date'] = pd.to_datetime(df['Date'])
    return df

# Authenticate with Google Cloud using a service account key.
def gcp_client_auth(key_json_file):
    try:
        bigquery_client = bigquery.Client.from_service_account_json(key_json_file)  
        return bigquery_client
    except Exception as e:
        print(f"Error connecting to Google Cloud: {e}")
        exit(1)  # Exit with an error code

# Données des Green Stocks avec plus de détails et de diversité
@st.cache_data
def load_stock_data():
    stocks = [
        {"Company": "NextEra Energy", "Ticker": "NEE", "Stock Exchange": "NYSE", "ESG Score": 83, "Main Focus": "Renewable Energy", "Region": "North America", "Pattern": "uptrend"},
        {"Company": "Vestas Wind Systems", "Ticker": "VWS.CO", "Stock Exchange": "Copenhagen", "ESG Score": 79, "Main Focus": "Wind Energy", "Region": "Europe", "Pattern": "cyclical"},
        {"Company": "First Solar", "Ticker": "FSLR", "Stock Exchange": "NASDAQ", "ESG Score": 75, "Main Focus": "Solar Energy", "Region": "North America", "Pattern": "volatile"},
        {"Company": "Tesla", "Ticker": "TSLA", "Stock Exchange": "NASDAQ", "ESG Score": 72, "Main Focus": "Electric Vehicles", "Region": "North America", "Pattern": "volatile"},
        {"Company": "Siemens Gamesa", "Ticker": "SGRE.MC", "Stock Exchange": "Madrid", "ESG Score": 71, "Main Focus": "Wind Energy", "Region": "Europe", "Pattern": "downtrend"},
        {"Company": "Ørsted", "Ticker": "ORSTED.CO", "Stock Exchange": "Copenhagen", "ESG Score": 84, "Main Focus": "Offshore Wind", "Region": "Europe", "Pattern": "uptrend"},
        {"Company": "Enphase Energy", "Ticker": "ENPH", "Stock Exchange": "NASDAQ", "ESG Score": 68, "Main Focus": "Solar Energy", "Region": "North America", "Pattern": "cyclical"},
        {"Company": "BYD Company", "Ticker": "1211.HK", "Stock Exchange": "Hong Kong", "ESG Score": 65, "Main Focus": "Electric Vehicles", "Region": "Asia", "Pattern": "uptrend"},
        {"Company": "Canadian Solar", "Ticker": "CSIQ", "Stock Exchange": "NASDAQ", "ESG Score": 69, "Main Focus": "Solar Energy", "Region": "North America", "Pattern": "downtrend"},
        {"Company": "SunPower", "Ticker": "SPWR", "Stock Exchange": "NASDAQ", "ESG Score": 70, "Main Focus": "Solar Energy", "Region": "North America", "Pattern": "volatile"},
        {"Company": "Xinyi Solar", "Ticker": "0968.HK", "Stock Exchange": "Hong Kong", "ESG Score": 66, "Main Focus": "Solar Energy", "Region": "Asia", "Pattern": "cyclical"},
        {"Company": "NIO", "Ticker": "NIO", "Stock Exchange": "NYSE", "ESG Score": 64, "Main Focus": "Electric Vehicles", "Region": "Asia", "Pattern": "volatile"}
    ]
    return pd.DataFrame(stocks)

# Données des Green Cryptocurrencies
@st.cache_data
def load_crypto_data():
    cryptos = [
        {"Coin": "Chia", "Type": "Proof of Space and Time", "Marketcap": "150M", "Electrical Power": "0.023 kW", 
         "Electricity Consumption (annualised)": "0.11 TWh", "CO₂ Emissions (annualised)": "56.7k tonnes", "Pattern": "volatile"},
        {"Coin": "Algorand", "Type": "Pure Proof of Stake", "Marketcap": "1.5B", "Electrical Power": "< 0.01 kW", 
         "Electricity Consumption (annualised)": "0.0002 TWh", "CO₂ Emissions (annualised)": "99 tonnes", "Pattern": "uptrend"},
        {"Coin": "Nano", "Type": "Open Representative Voting", "Marketcap": "105M", "Electrical Power": "0.000112 kW", 
         "Electricity Consumption (annualised)": "0.000001 TWh", "CO₂ Emissions (annualised)": "0.5 tonnes", "Pattern": "cyclical"},
        {"Coin": "IOTA", "Type": "Directed Acyclic Graph", "Marketcap": "500M", "Electrical Power": "0.0001 kW", 
         "Electricity Consumption (annualised)": "0.00004 TWh", "CO₂ Emissions (annualised)": "18 tonnes", "Pattern": "downtrend"},
        {"Coin": "Cardano", "Type": "Proof of Stake", "Marketcap": "9B", "Electrical Power": "0.006 kW", 
         "Electricity Consumption (annualised)": "0.006 TWh", "CO₂ Emissions (annualised)": "2.8k tonnes", "Pattern": "volatile"},
        {"Coin": "Polkadot", "Type": "Nominated Proof of Stake", "Marketcap": "5.3B", "Electrical Power": "0.008 kW", 
         "Electricity Consumption (annualised)": "0.008 TWh", "CO₂ Emissions (annualised)": "3.6k tonnes", "Pattern": "uptrend"},
        {"Coin": "Tezos", "Type": "Liquid Proof of Stake", "Marketcap": "720M", "Electrical Power": "0.003 kW", 
         "Electricity Consumption (annualised)": "0.004 TWh", "CO₂ Emissions (annualised)": "1.9k tonnes", "Pattern": "cyclical"},
        {"Coin": "Stellar", "Type": "Stellar Consensus Protocol", "Marketcap": "2.7B", "Electrical Power": "0.0002 kW", 
         "Electricity Consumption (annualised)": "0.0003 TWh", "CO₂ Emissions (annualised)": "142 tonnes", "Pattern": "downtrend"}
    ]
    return pd.DataFrame(cryptos)

# Fonction pour attribuer des couleurs basées sur le score ESG
def get_esg_color(score):
    if score >= 80:
        return "#1E8449"  # Vert foncé
    elif score >= 70:
        return "#27AE60"  # Vert moyen
    elif score >= 60:
        return "#82E0AA"  # Vert clair
    else:
        return "#F4D03F"  # Jaune

# Fonction pour créer une étiquette formattée pour les métriques
def format_metric_label(label, value, unit=""):
    return f"<span style='font-size:0.8rem;color:#6c757d;'>{label}</span><br><span style='font-size:1.2rem;font-weight:600;'>{value}{unit}</span>"

# Chargement des données
stocks_df = load_stock_data()
crypto_df = load_crypto_data()

# Titre principal avec animation
st.markdown("<p class='main-header'>🌿 Green Investment Dashboard</p>", unsafe_allow_html=True)
st.markdown("<p style='text-align:center;color:#6c757d;margin-bottom:2rem;'>Analyse des investissements durables et écologiques</p>", unsafe_allow_html=True)

# Tabs pour passer entre stocks et crypto
tab1, tab2 = st.tabs(["🏢 Green Stocks", "💰 Green Cryptocurrencies"])

# Section 1: Green Stocks
with tab1:
    st.markdown("<p class='sub-header'>Green Stocks</p>", unsafe_allow_html=True)
    
    # Filtres dans un container stylisé
    with st.container():
        st.markdown("<div class='filter-section'>", unsafe_allow_html=True)
        col1, col2, col3 = st.columns(3)
        with col1:
            region_filter = st.multiselect("Filtrer par région", 
                                          options=stocks_df["Region"].unique(), 
                                          default=stocks_df["Region"].unique(),
                                          key="region_filter")
        with col2:
            focus_filter = st.multiselect("Filtrer par activité", 
                                         options=stocks_df["Main Focus"].unique(), 
                                         default=stocks_df["Main Focus"].unique(),
                                         key="focus_filter")
        with col3:
            esg_min = st.slider("Score ESG minimum", 
                               min_value=60, 
                               max_value=90, 
                               value=60,
                               key="esg_slider")
        st.markdown("</div>", unsafe_allow_html=True)
    
    # Application des filtres
    filtered_stocks = stocks_df[
        (stocks_df["Region"].isin(region_filter)) &
        (stocks_df["Main Focus"].isin(focus_filter)) &
        (stocks_df["ESG Score"] >= esg_min)
    ]
    
    # Affichage des métriques clés dans des cards modernisées
    if not filtered_stocks.empty:
        st.markdown("<p style='font-size:1.2rem;font-weight:600;margin-top:1rem;'>Key Metrics</p>", unsafe_allow_html=True)
        
        # Calcul du nombre de colonnes (max 4 par ligne)
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
                        st.markdown(f"""
                        <div class='metric-card'>
                            <p class='company-name'>{stock['Company']}</p>
                            <p class='ticker'>{stock['Ticker']}</p>
                            <p class='esg-score'>ESG: {stock['ESG Score']}/100</p>
                            <p class='focus-tag'>{stock['Main Focus']}</p>
                            <p class='region-exchange'>{stock['Stock Exchange']} · {stock['Region']}</p>
                        </div>
                        """, unsafe_allow_html=True)
        
        # Sélecteur pour le graphique de stocks avec layout amélioré
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
        
        # Conversion de la période sélectionnée en jours
        if time_range == "1M":
            days = 30
        elif time_range == "3M":
            days = 90
        elif time_range == "6M":
            days = 180
        elif time_range == "1A":
            days = 365
        else:  # Tout
            days = 365
        
        selected_ticker = selected_stock.split(" - ")[0]
        selected_company = selected_stock.split(" - ")[1]
        
        # Récupérer le pattern pour ce stock
        stock_pattern = filtered_stocks[filtered_stocks["Ticker"] == selected_ticker]["Pattern"].values[0]
        
        # Génération et affichage des données historiques pour le stock sélectionné
        historical_stock_data = generate_historical_data(selected_ticker, days=days, pattern=stock_pattern)
        
        # Configuration des couleurs en fonction du pattern
        if stock_pattern == "uptrend":
            line_color = "#27AE60"  # vert vif
        elif stock_pattern == "downtrend":
            line_color = "#E74C3C"  # rouge
        elif stock_pattern == "volatile":
            line_color = "#F39C12"  # orange
        elif stock_pattern == "cyclical":
            line_color = "#3498DB"  # bleu
        else:
            line_color = "#2C3E50"  # gris foncé
        
        # Graphique principal avec des annotations pour les événements clés
        fig = go.Figure()
        
        # Ligne principale
        fig.add_trace(go.Scatter(
            x=historical_stock_data['Date'],
            y=historical_stock_data['Price'],
            mode='lines',
            name=selected_ticker,
            line=dict(color=line_color, width=2.5),
            hovertemplate='Date: %{x}<br>Price: $%{y:.2f}<extra></extra>'
        ))
        
        # Ajout de moyenne mobile pour l'analyse
        historical_stock_data['MA20'] = historical_stock_data['Price'].rolling(window=20).mean()
        historical_stock_data['MA50'] = historical_stock_data['Price'].rolling(window=50).mean()
        
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
        
        # Annotations pour les événements fictifs clés (pour démonstration)
        if len(historical_stock_data) > 100:
            # Choisir quelques points aléatoires pour les annotations
            annotation_points = sorted(random.sample(range(20, len(historical_stock_data) - 20), 3))
            
            events = [
                "Annonce ESG",
                "Expansion des opérations",
                "Nouveau projet vert"
            ]
            
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
        
        # Mise en forme du graphique
        fig.update_layout(
            title=f"{selected_company} ({selected_ticker}) - Évolution du cours",
            xaxis_title="Date",
            yaxis_title="Prix (USD)",
            height=500,
            template="plotly_white",
            hovermode="x unified",
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=1.02,
                xanchor="right",
                x=1
            ),
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
        
        # Ajouter des zones de prix importantes
        min_price = historical_stock_data['Price'].min()
        max_price = historical_stock_data['Price'].max()
        
        fig.add_shape(
            type="rect",
            xref="paper", yref="y",
            x0=0, y0=min_price,
            x1=1, y1=min_price * 1.1,
            fillcolor="rgba(242, 242, 242, 0.5)",
            line_width=0
        )
        
        fig.add_shape(
            type="rect",
            xref="paper", yref="y",
            x0=0, y0=max_price * 0.9,
            x1=1, y1=max_price,
            fillcolor="rgba(242, 242, 242, 0.5)",
            line_width=0
        )
        
        st.plotly_chart(fig, use_container_width=True)
        
        # Graphiques supplémentaires dans des onglets
        st.markdown("</div>", unsafe_allow_html=True)
        
        chart_tabs = st.tabs(["Volume", "Comparaison", "Analyse"])
        
        with chart_tabs[0]:
            st.markdown("<div class='chart-container'>", unsafe_allow_html=True)
            # Volume avec gradient de couleur
            volume_colors = []
            for i in range(len(historical_stock_data)):
                if i > 0 and historical_stock_data.iloc[i]['Price'] > historical_stock_data.iloc[i-1]['Price']:
                    volume_colors.append("rgba(39, 174, 96, 0.7)")  # vert pour hausse
                else:
                    volume_colors.append("rgba(231, 76, 60, 0.7)")  # rouge pour baisse
            
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
                yaxis=dict(
                    type='log',
                    title_font=dict(size=14)
                )
            )
            st.plotly_chart(volume_fig, use_container_width=True)
            st.markdown("</div>", unsafe_allow_html=True)
        
        with chart_tabs[1]:
            st.markdown("<div class='chart-container'>", unsafe_allow_html=True)
            # Comparaison avec des concurrents
            competitors = filtered_stocks[
                (filtered_stocks["Main Focus"] == stocks_df[stocks_df["Ticker"] == selected_ticker]["Main Focus"].values[0]) &
                (filtered_stocks["Ticker"] != selected_ticker)
            ].head(3)
            
            if not competitors.empty:
                comp_fig = go.Figure()
                
                # Normaliser les prix à 100 pour la comparaison
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
                
                # Ajouter jusqu'à 3 concurrents
                colors = ["#3498DB", "#9B59B6", "#F1C40F"]
                for i, (_, comp) in enumerate(competitors.iterrows()):
                    comp_data = generate_historical_data(comp["Ticker"], days=days, pattern=comp["Pattern"])
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
                    legend=dict(
                        orientation="h",
                        yanchor="bottom",
                        y=1.02,
                        xanchor="right",
                        x=1
                    )
                )
                st.plotly_chart(comp_fig, use_container_width=True)
            else:
                st.info("Pas assez de données pour effectuer une comparaison avec des concurrents.")
            st.markdown("</div>", unsafe_allow_html=True)
            
        with chart_tabs[2]:
            st.markdown("<div class='chart-container'>", unsafe_allow_html=True)
            # Statistiques et analyse
            col1, col2 = st.columns(2)
            
            with col1:
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
                    <p><b>Score ESG:</b> {stocks_df[stocks_df['Ticker'] == selected_ticker]['ESG Score'].values[0]}/100</p>
                </div>
                """, unsafe_allow_html=True)
                
            with col2:
                # Calcul des variations
                historical_stock_data['daily_return'] = historical_stock_data['Price'].pct_change() * 100
                volatility = historical_stock_data['daily_return'].std()
                
                # Graphique de distribution des rendements
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
    
    # Filtres pour les cryptomonnaies
    with st.container():
        st.markdown("<div class='filter-section'>", unsafe_allow_html=True)
        col1, col2 = st.columns(2)
        with col1:
            type_filter = st.multiselect("Filtrer par type de consensus", 
                                       options=crypto_df["Type"].unique(), 
                                       default=crypto_df["Type"].unique(),
                                       key="crypto_type_filter")
        with col2:
            # Filtre basé sur la consommation électrique (valeur convertie en nombre)
            power_max = st.slider("Consommation électrique max (kW)", 
                              min_value=0.0, 
                              max_value=0.025, 
                              value=0.025, 
                              step=0.001,
                              format="%.3f",
                              key="power_slider")
        st.markdown("</div>", unsafe_allow_html=True)
    
    # Conversion de 'Electrical Power' en nombres pour le filtre
    crypto_df['Power_Numeric'] = crypto_df['Electrical Power'].str.extract(r'([\d.]+)').astype(float)
    
    # Application des filtres
    filtered_cryptos = crypto_df[
        (crypto_df["Type"].isin(type_filter)) &
        (crypto_df["Power_Numeric"] <= power_max)
    ]
    
    if not filtered_cryptos.empty:
        # Affichage des cryptomonnaies dans des cartes
        st.markdown("<p style='font-size:1.2rem;font-weight:600;margin-top:1rem;'>Cryptomonnaies écologiques</p>", unsafe_allow_html=True)
        
        # Distribution en colonnes
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
                            <p style='font-size:0.9rem;margin-top:0.5rem;'><b>Électricité:</b> {crypto['Electrical Power']}</p>
                            <p style='font-size:0.9rem;'><b>CO₂:</b> {crypto['CO₂ Emissions (annualised)']}</p>
                        </div>
                        """, unsafe_allow_html=True)
        
        # Sélection et visualisation de crypto
        st.markdown("<div class='chart-container'>", unsafe_allow_html=True)
        selected_crypto = st.selectbox(
            "Sélectionnez une cryptomonnaie pour voir les données historiques", 
            filtered_cryptos["Coin"],
            key="crypto_selector"
        )
        
        # Générer des données historiques pour la crypto sélectionnée
        crypto_pattern = filtered_cryptos[filtered_cryptos["Coin"] == selected_crypto]["Pattern"].values[0]
        historical_crypto_data = generate_historical_data(selected_crypto, days=365, pattern=crypto_pattern)
        
        # Définir la couleur du graphique en fonction du pattern
        if crypto_pattern == "uptrend":
            crypto_color = "#27AE60"  # vert vif
        elif crypto_pattern == "downtrend":
            crypto_color = "#E74C3C"  # rouge
        elif crypto_pattern == "volatile":
            crypto_color = "#F39C12"  # orange
        elif crypto_pattern == "cyclical":
            crypto_color = "#3498DB"  # bleu
        else:
            crypto_color = "#2C3E50"  # gris foncé
        
        # Graphique principal
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
        
        # Tableau comparatif de l'impact environnemental
        st.markdown("<p class='sub-header'>Impact environnemental comparatif</p>", unsafe_allow_html=True)
        
        # Préparation des données pour le graphique comparatif
        impact_fig = go.Figure()
        
        # Crypto filtrées triées par consommation
        sorted_cryptos = filtered_cryptos.sort_values(by="Power_Numeric")
        
        impact_fig.add_trace(go.Bar(
            x=sorted_cryptos["Coin"],
            y=sorted_cryptos["Power_Numeric"],
            marker_color="#27AE60",
            name="Consommation (kW)",
            hovertemplate='Crypto: %{x}<br>Consommation: %{y:.6f} kW<extra></extra>'
        ))
        
        impact_fig.update_layout(
            title="Comparaison de la consommation électrique",
            xaxis_title="Cryptomonnaie",
            yaxis_title="Consommation (kW)",
            height=400,
            template="plotly_white",
            yaxis=dict(type="log")  # Échelle logarithmique pour mieux visualiser les petites valeurs
        )
        
        st.plotly_chart(impact_fig, use_container_width=True)
        
        # Tableau d'information
        st.markdown("<div class='chart-container'>", unsafe_allow_html=True)
        st.markdown("""
        <h3 style='font-size:1.3rem;margin-bottom:1rem;'>Pourquoi ces cryptomonnaies sont considérées comme écologiques</h3>
        <p>Ces cryptomonnaies utilisent des mécanismes de consensus alternatifs qui consomment considérablement moins d'énergie que les cryptomonnaies traditionnelles basées sur le Proof of Work (preuve de travail).</p>
        """, unsafe_allow_html=True)
        
        # Table de comparaison
        st.table(filtered_cryptos[["Coin", "Type", "Electricity Consumption (annualised)", "CO₂ Emissions (annualised)"]])
        st.markdown("</div>", unsafe_allow_html=True)
    else:
        st.warning("Aucune cryptomonnaie ne correspond aux filtres sélectionnés.")

# Divider
st.markdown("<div class='gradient-divider'></div>", unsafe_allow_html=True)

# Footer avec disclaimer
st.markdown("""
<div class='footer'>
    <p style='font-weight:600;'>Green Investment Dashboard</p>
    <p class='small-text'>Ce tableau de bord est fourni uniquement à des fins d'illustration. Les données présentées sont générées aléatoirement et ne constituent pas des conseils d'investissement.</p>
</div>
""", unsafe_allow_html=True)