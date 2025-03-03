-- models/staging/stg_finnhub_financials.sql
WITH source AS (
    SELECT * FROM {{ source('raw', 'raw_finnhub_financials') }}
)

SELECT
    -- Normalisation du ticker
    CASE
        WHEN ticker IS NULL THEN NULL
        WHEN TRIM(ticker) = '' THEN NULL
        ELSE TRIM(UPPER(ticker))  -- Généralement les tickers sont en majuscules
    END AS ticker,
    
    -- Conversion robuste du prix courant
    CASE
        WHEN current_price IS NULL THEN NULL
        WHEN TRIM(CAST(current_price AS STRING)) = '' THEN NULL
        WHEN TRIM(CAST(current_price AS STRING)) = '0' OR current_price = 0 THEN 0
        -- Gestion des valeurs avec formats potentiellement problématiques
        WHEN REGEXP_CONTAINS(CAST(current_price AS STRING), r'[^0-9.\-e]') THEN
            SAFE_CAST(REGEXP_REPLACE(CAST(current_price AS STRING), r'[^0-9.\-e]', '') AS FLOAT64)
        -- Gestion de la notation scientifique
        WHEN REGEXP_CONTAINS(CAST(current_price AS STRING), r'^\d+\.\d+e[+-]\d+$') THEN
            SAFE_CAST(current_price AS FLOAT64)
        ELSE 
            SAFE_CAST(current_price AS FLOAT64)
    END AS current_price,
    
    -- Conversion robuste de la capitalisation boursière
    CASE
        WHEN marketCapitalization IS NULL THEN NULL
        WHEN TRIM(CAST(marketCapitalization AS STRING)) = '' THEN NULL
        WHEN TRIM(CAST(marketCapitalization AS STRING)) = '0' OR marketCapitalization = 0 THEN 0
        -- Gestion des valeurs avec format potentiellement monétaire (ex: $1,234.56)
        WHEN REGEXP_CONTAINS(CAST(marketCapitalization AS STRING), r'[$,]') THEN
            SAFE_CAST(REGEXP_REPLACE(REGEXP_REPLACE(CAST(marketCapitalization AS STRING), r'[$,]', ''), r'[^0-9.\-e]', '') AS FLOAT64)
        -- Gestion des valeurs avec autres caractères non numériques
        WHEN REGEXP_CONTAINS(CAST(marketCapitalization AS STRING), r'[^0-9.\-e]') THEN
            SAFE_CAST(REGEXP_REPLACE(CAST(marketCapitalization AS STRING), r'[^0-9.\-e]', '') AS FLOAT64)
        -- Gestion de la notation scientifique
        WHEN REGEXP_CONTAINS(CAST(marketCapitalization AS STRING), r'^\d+\.\d+e[+-]\d+$') THEN
            SAFE_CAST(marketCapitalization AS FLOAT64)
        ELSE 
            SAFE_CAST(marketCapitalization AS FLOAT64)
    END AS market_cap,
    
    -- Timestamp de chargement
    CURRENT_TIMESTAMP() AS loaded_at
FROM source