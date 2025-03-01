-- models/marts/crypto_mart/fact_crypto_environmental.sql
WITH crypto_metrics AS (
    SELECT * FROM {{ ref('int_crypto_metrics') }}
)

SELECT
    coin_name,
    symbol,
    electricity_consumption_gw,
    co2_emissions_mt,
    co2_emissions_tonnes,
    market_cap_per_co2,
    -- Normalisation des émissions par rapport à Bitcoin comme référence
    CASE
        WHEN symbol = 'btc' THEN 1.0
        ELSE SAFE_CAST(
            co2_emissions_mt / NULLIF((SELECT MAX(co2_emissions_mt) FROM crypto_metrics WHERE symbol = 'btc'), 0)
            AS FLOAT64)
    END AS relative_emissions_to_bitcoin,
    
    -- Efficacité énergétique: USD par kWh
    CASE
        WHEN electricity_consumption_gw > 0
        THEN SAFE_CAST(market_cap / (electricity_consumption_gw * 1000000) AS FLOAT64)  -- Convert GW to kW
        ELSE NULL
    END AS market_cap_per_kwh,
    
    -- Score d'empreinte écologique (note de 0 à 100, inversement proportionnelle aux émissions)
    CASE
        WHEN co2_emissions_mt IS NULL THEN NULL
        WHEN co2_emissions_mt = 0 THEN 100  -- Parfaitement écologique
        ELSE SAFE_CAST(
            100 * (1 - LEAST(co2_emissions_mt / NULLIF((SELECT MAX(co2_emissions_mt) FROM crypto_metrics), 0.0001), 1))
            AS FLOAT64)
    END AS ecological_score,
    
    -- Catégorie d'investissement écologique
    CASE
        WHEN co2_emissions_mt IS NULL THEN 'Données insuffisantes'
        WHEN co2_emissions_mt = 0 THEN 'Impact zéro'
        WHEN co2_emissions_mt < 0.000001 THEN 'Impact négligeable'
        WHEN co2_emissions_mt < 0.00001 THEN 'Très faible impact'
        WHEN co2_emissions_mt < 0.0001 THEN 'Faible impact'
        WHEN co2_emissions_mt < 0.001 THEN 'Impact modéré'
        ELSE 'Impact élevé'
    END AS ecological_category,
    
    environmental_impact_category
FROM crypto_metrics