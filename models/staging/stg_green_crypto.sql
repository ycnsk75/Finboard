-- models/staging/stg_green_crypto.sql
WITH source AS (
    SELECT * FROM {{ source('raw', 'raw_green_crypto') }}
)

SELECT
    -- Normalisation du nom de la cryptomonnaie
    CASE
        WHEN Coin IS NULL THEN NULL
        WHEN TRIM(Coin) = '' THEN NULL
        ELSE TRIM(Coin)
    END AS coin_name,
    
    -- Normalisation du symbole
    CASE
        WHEN Symbol IS NULL THEN NULL
        WHEN TRIM(Symbol) = '' THEN NULL
        ELSE TRIM(LOWER(Symbol))
    END AS symbol,
    
    -- Conversion robuste du prix
    CASE
        WHEN Price IS NULL THEN NULL
        WHEN TRIM(CAST(Price AS STRING)) = '' THEN NULL
        WHEN TRIM(CAST(Price AS STRING)) = '0' OR Price = 0 THEN 0
        ELSE SAFE_CAST(Price AS FLOAT64)
    END AS price,
    
    -- Conversion robuste du changement absolu
    CASE
        WHEN Change IS NULL THEN NULL
        WHEN TRIM(CAST(Change AS STRING)) = '' THEN NULL
        WHEN TRIM(CAST(Change AS STRING)) = '0' OR Change = 0 THEN 0
        ELSE SAFE_CAST(Change AS FLOAT64)
    END AS price_change,
    
    -- Conversion robuste du pourcentage de changement
    CASE
        WHEN Percent_Change IS NULL THEN NULL
        WHEN TRIM(CAST(Percent_Change AS STRING)) = '' THEN NULL
        WHEN TRIM(CAST(Percent_Change AS STRING)) = '0' OR Percent_Change = 0 THEN 0
        -- Gestion des valeurs avec le symbole %
        WHEN REGEXP_CONTAINS(CAST(Percent_Change AS STRING), r'%') THEN
            SAFE_CAST(REGEXP_REPLACE(CAST(Percent_Change AS STRING), r'%', '') AS FLOAT64)
        ELSE SAFE_CAST(Percent_Change AS FLOAT64)
    END AS percent_change,
    
    -- Conversion robuste du prix haut sur 24h
    CASE
        WHEN High_24h IS NULL THEN NULL
        WHEN TRIM(CAST(High_24h AS STRING)) = '' THEN NULL
        WHEN TRIM(CAST(High_24h AS STRING)) = '0' OR High_24h = 0 THEN 0
        ELSE SAFE_CAST(High_24h AS FLOAT64)
    END AS high_24h,
    
    -- Conversion robuste du prix bas sur 24h
    CASE
        WHEN Low_24h IS NULL THEN NULL
        WHEN TRIM(CAST(Low_24h AS STRING)) = '' THEN NULL
        WHEN TRIM(CAST(Low_24h AS STRING)) = '0' OR Low_24h = 0 THEN 0
        ELSE SAFE_CAST(Low_24h AS FLOAT64)
    END AS low_24h,
    
    -- Conversion robuste du prix d'ouverture sur 24h
    CASE
        WHEN Open_24h IS NULL THEN NULL
        WHEN TRIM(CAST(Open_24h AS STRING)) = '' THEN NULL
        WHEN TRIM(CAST(Open_24h AS STRING)) = '0' OR Open_24h = 0 THEN 0
        ELSE SAFE_CAST(Open_24h AS FLOAT64)
    END AS open_24h,
    
    -- Conversion robuste du prix de clôture précédent
    CASE
        WHEN Previous_Close_24h IS NULL THEN NULL
        WHEN TRIM(CAST(Previous_Close_24h AS STRING)) = '' THEN NULL
        WHEN TRIM(CAST(Previous_Close_24h AS STRING)) = '0' OR Previous_Close_24h = 0 THEN 0
        ELSE SAFE_CAST(Previous_Close_24h AS FLOAT64)
    END AS previous_close_24h,
    
    -- Conversion robuste du timestamp
    CASE
        WHEN Timestamp IS NULL THEN NULL
        WHEN TRIM(CAST(Timestamp AS STRING)) = '' THEN NULL
        ELSE SAFE_CAST(Timestamp AS INT64)
    END AS timestamp,
    
    -- Timestamp de chargement
    CURRENT_TIMESTAMP() AS loaded_at
FROM source