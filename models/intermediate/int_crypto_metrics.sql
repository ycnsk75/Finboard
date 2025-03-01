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
    cp.price_usd,
    gc.price AS green_price,
    gc.percent_change,
    cc.market_cap,
    cc.electricity_consumption_gw,
    cc.co2_emissions_mt,
    -- Calcul des métriques environnementales
    CASE
        WHEN cc.market_cap > 0 AND cc.co2_emissions_mt > 0 
        THEN cc.market_cap / cc.co2_emissions_mt
        ELSE NULL
    END AS market_cap_per_co2,
    CASE
        WHEN cc.co2_emissions_mt > 0
        THEN cc.co2_emissions_mt * 1000000 -- Conversion en tonnes
        ELSE NULL
    END AS co2_emissions_tonnes,
    -- Classification écologique
    CASE
        WHEN cc.co2_emissions_mt < 0.0001 THEN 'Très faible impact'
        WHEN cc.co2_emissions_mt < 0.001 THEN 'Faible impact'
        WHEN cc.co2_emissions_mt < 0.01 THEN 'Impact modéré'
        WHEN cc.co2_emissions_mt < 0.1 THEN 'Impact élevé'
        ELSE 'Impact très élevé'
    END AS environmental_impact_category
FROM crypto_carbon cc
LEFT JOIN crypto_prices cp ON LOWER(cc.symbol) = LOWER(cp.coin)
LEFT JOIN green_crypto gc ON LOWER(cc.symbol) = LOWER(gc.symbol)

