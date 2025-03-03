-- models/marts/green_finance/green_investment_overview.sql
WITH crypto_dim AS (
    SELECT * FROM {{ ref('dim_cryptocurrencies') }}
),

crypto_fact AS (
    SELECT * FROM {{ ref('fact_crypto_environmental') }}
),

stock_dim AS (
    SELECT * FROM {{ ref('dim_companies') }}
),

stock_fact AS (
    SELECT * FROM {{ ref('fact_stock_environmental') }}
)

-- Données des cryptomonnaies
SELECT
    'Cryptocurrency' AS asset_type,
    c.coin_name AS asset_name,
    c.symbol AS asset_symbol,
    c.price_usd AS current_price,
    c.market_cap,
    cf.co2_emissions_mt,
    cf.electricity_consumption_gw AS energy_consumption,
    cf.market_cap_per_co2 AS green_efficiency,
    cf.ecological_score AS environmental_score,
    cf.ecological_category AS environmental_category,
    -- Attribuer une catégorie d'actif plus spécifique
    CASE
        WHEN c.symbol IN ('btc', 'eth', 'ltc', 'xrp', 'bch') THEN 'Crypto Majeure'
        ELSE 'Crypto Alternative'
    END AS asset_subtype,
    -- Estimer un "ESG" pour les cryptos (principalement la partie environnementale)
    SAFE_CAST(cf.ecological_score AS INT64) AS estimated_esg,
    NULL AS region,
    NULL AS year_founded,
    'Digital Asset' AS main_focus
FROM crypto_dim c
JOIN crypto_fact cf ON c.coin_name = cf.coin_name

UNION ALL

-- Données des actions vertes
SELECT
    'Stock' AS asset_type,
    s.company_name AS asset_name,
    s.ticker AS asset_symbol,
    s.current_price,
    s.market_cap,
    sf.co2_emissions_mt,
    SAFE_CAST(sf.electricity_consumption_gwh / 1000 AS FLOAT64) AS energy_consumption, -- Converti de GWh en GW pour homogénéité
    sf.market_cap_per_co2 AS green_efficiency,
    sf.normalized_environmental_score AS environmental_score,
    sf.environmental_performance_category AS environmental_category,
    s.category AS asset_subtype,
    s.esg_score AS estimated_esg,
    s.region,
    s.year_founded,
    s.main_focus
FROM stock_dim s
JOIN stock_fact sf ON s.id = sf.id