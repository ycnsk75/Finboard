-- models/intermediate/int_crypto_metrics.sql
WITH crypto_prices AS (
    SELECT * FROM {{ ref('stg_coincap_prices') }}
),

green_crypto AS (
    SELECT * FROM {{ ref('stg_green_crypto') }}
),

crypto_carbon AS (
    SELECT * FROM {{ ref('stg_green_crypto_carbon') }}
)

SELECT
    COALESCE(cc.coin_name, cp.coin) AS coin_name,
    COALESCE(cc.symbol, LOWER(cp.coin)) AS symbol,
    SAFE_CAST(cp.price_usd AS FLOAT64) AS price_usd,
    SAFE_CAST(gc.price AS FLOAT64) AS green_price,
    SAFE_CAST(gc.percent_change AS FLOAT64) AS percent_change,
    SAFE_CAST(cc.market_cap AS FLOAT64) AS market_cap,
    cc.electricity_consumption_gw,
    cc.co2_emissions_mt,
    -- Calcul des métriques environnementales
    CASE
        WHEN SAFE_CAST(cc.market_cap AS FLOAT64) > 0 AND SAFE_CAST(cc.co2_emissions_mt AS FLOAT64) > 0 
        THEN SAFE_CAST(cc.market_cap AS FLOAT64) / SAFE_CAST(cc.co2_emissions_mt AS FLOAT64)
        ELSE NULL
    END AS market_cap_per_co2,
    CASE
        WHEN SAFE_CAST(cc.co2_emissions_mt AS FLOAT64) > 0
        THEN SAFE_CAST(cc.co2_emissions_mt AS FLOAT64) * 1000000 -- Conversion en tonnes
        ELSE NULL
    END AS co2_emissions_tonnes,
    -- Classification écologique
    CASE
        WHEN SAFE_CAST(cc.co2_emissions_mt AS FLOAT64) < 0.0001 THEN 'Très faible impact'
        WHEN SAFE_CAST(cc.co2_emissions_mt AS FLOAT64) < 0.001 THEN 'Faible impact'
        WHEN SAFE_CAST(cc.co2_emissions_mt AS FLOAT64) < 0.01 THEN 'Impact modéré'
        WHEN SAFE_CAST(cc.co2_emissions_mt AS FLOAT64) < 0.1 THEN 'Impact élevé'
        ELSE 'Impact très élevé'
    END AS environmental_impact_category
FROM crypto_carbon cc
LEFT JOIN crypto_prices cp ON LOWER(cc.symbol) = LOWER(cp.coin)
LEFT JOIN green_crypto gc ON LOWER(cc.symbol) = LOWER(gc.symbol)