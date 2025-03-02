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

-- Utiliser green_crypto comme table principale car elle contient les deux informations
SELECT
    COALESCE(gc.coin_name, cc.coin_name) AS coin_name,
    COALESCE(gc.symbol, cc.symbol) AS symbol,
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
FROM green_crypto gc
LEFT JOIN crypto_carbon cc ON TRIM(LOWER(gc.symbol)) = TRIM(LOWER(cc.symbol))
LEFT JOIN crypto_prices cp ON TRIM(LOWER(gc.coin_name)) = TRIM(LOWER(cp.coin))