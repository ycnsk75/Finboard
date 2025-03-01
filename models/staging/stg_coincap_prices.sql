-- models/staging/stg_coincap_prices.sql
WITH source AS (
    SELECT * FROM {{ source('raw', 'raw_coincap_prices') }}
)

SELECT
    -- Normalisation du nom de la crypto-monnaie
    CASE
        WHEN coin IS NULL THEN NULL
        WHEN TRIM(coin) = '' THEN NULL
        ELSE TRIM(LOWER(coin))
    END AS coin,
    
    -- Conversion robuste du prix en USD
    CASE
        WHEN price_usd IS NULL THEN NULL
        WHEN TRIM(CAST(price_usd AS STRING)) = '' THEN NULL
        WHEN TRIM(CAST(price_usd AS STRING)) = '0' THEN 0
        -- Gestion des valeurs avec formats potentiellement problématiques
        WHEN REGEXP_CONTAINS(CAST(price_usd AS STRING), r'[^0-9.\-e]') THEN
            SAFE_CAST(REGEXP_REPLACE(CAST(price_usd AS STRING), r'[^0-9.\-e]', '') AS FLOAT64)
        -- Gestion de la notation scientifique
        WHEN REGEXP_CONTAINS(CAST(price_usd AS STRING), r'^\d+\.\d+e[+-]\d+$') THEN
            SAFE_CAST(price_usd AS FLOAT64)
        ELSE 
            SAFE_CAST(price_usd AS FLOAT64)
    END AS price_usd,
    
    -- Timestamp de chargement
    CURRENT_TIMESTAMP() AS loaded_at
FROM source