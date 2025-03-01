-- models/staging/stg_green_stock.sql
WITH source AS (
    SELECT * FROM {{ source('raw', 'raw_green_stock') }}
)

SELECT
    -- Conversion sécurisée de l'ID
    SAFE_CAST(ID AS INT64) AS id,
    
    -- Normalisation du nom de l'entreprise
    CASE
        WHEN Company IS NULL THEN NULL
        WHEN TRIM(Company) = '' THEN NULL
        ELSE TRIM(Company)
    END AS company_name,
    
    -- Normalisation du ticker
    CASE
        WHEN Ticker IS NULL THEN NULL
        WHEN TRIM(Ticker) = '' THEN NULL
        ELSE TRIM(UPPER(Ticker))  -- Standardisation en majuscules
    END AS ticker,
    
    -- Normalisation de la catégorie
    CASE
        WHEN Category IS NULL THEN NULL
        WHEN TRIM(Category) = '' THEN NULL
        ELSE TRIM(Category)
    END AS category,
    
    -- Normalisation de la place boursière
    CASE
        WHEN Stock_Exchange IS NULL THEN NULL
        WHEN TRIM(Stock_Exchange) = '' THEN NULL
        ELSE TRIM(Stock_Exchange)
    END AS stock_exchange,
    
    -- Conversion robuste de la capitalisation boursière
    CASE
        WHEN Marketcap IS NULL THEN NULL
        WHEN TRIM(CAST(Marketcap AS STRING)) = '' THEN NULL
        WHEN TRIM(CAST(Marketcap AS STRING)) = '0' THEN 0
        -- Format avec symbole monétaire et séparateurs de milliers
        WHEN REGEXP_CONTAINS(CAST(Marketcap AS STRING), r'[$,]') THEN
            SAFE_CAST(REGEXP_REPLACE(REGEXP_REPLACE(CAST(Marketcap AS STRING), r'[$,]', ''), r'[^0-9.\-e]', '') AS FLOAT64)
        -- Format avec des suffixes K, M, B
        WHEN REGEXP_CONTAINS(LOWER(CAST(Marketcap AS STRING)), r'[kmb]$') THEN
            CASE
                WHEN ENDS_WITH(LOWER(CAST(Marketcap AS STRING)), 'k') THEN 
                    SAFE_CAST(REGEXP_REPLACE(LOWER(CAST(Marketcap AS STRING)), r'k$', '') AS FLOAT64) * 1000
                WHEN ENDS_WITH(LOWER(CAST(Marketcap AS STRING)), 'm') THEN 
                    SAFE_CAST(REGEXP_REPLACE(LOWER(CAST(Marketcap AS STRING)), r'm$', '') AS FLOAT64) * 1000000
                WHEN ENDS_WITH(LOWER(CAST(Marketcap AS STRING)), 'b') THEN 
                    SAFE_CAST(REGEXP_REPLACE(LOWER(CAST(Marketcap AS STRING)), r'b$', '') AS FLOAT64) * 1000000000
                ELSE SAFE_CAST(Marketcap AS FLOAT64)
            END
        ELSE SAFE_CAST(Marketcap AS FLOAT64)
    END AS market_cap,
    
    -- Conversion robuste du score ESG
    CASE
        WHEN ESG_Score IS NULL THEN NULL
        WHEN TRIM(CAST(ESG_Score AS STRING)) = '' THEN NULL
        ELSE SAFE_CAST(ESG_Score AS INT64)
    END AS esg_score,
    
    -- Conversion robuste du chiffre d'affaires
    CASE
        WHEN Revenue IS NULL THEN NULL
        WHEN TRIM(CAST(Revenue AS STRING)) = '' THEN NULL
        WHEN TRIM(CAST(Revenue AS STRING)) = '0' THEN 0
        -- Format avec symbole monétaire et séparateurs de milliers
        WHEN REGEXP_CONTAINS(CAST(Revenue AS STRING), r'[$,]') THEN
            SAFE_CAST(REGEXP_REPLACE(REGEXP_REPLACE(CAST(Revenue AS STRING), r'[$,]', ''), r'[^0-9.\-e]', '') AS FLOAT64)
        -- Format avec suffixes B (billions)
        WHEN REGEXP_CONTAINS(LOWER(CAST(Revenue AS STRING)), r'b$') THEN
            SAFE_CAST(REGEXP_REPLACE(LOWER(CAST(Revenue AS STRING)), r'b$', '') AS FLOAT64) * 1000000000
        ELSE SAFE_CAST(Revenue AS FLOAT64)
    END AS revenue_billions,
    
    -- Normalisation de l'activité principale
    CASE
        WHEN Main_Focus IS NULL THEN NULL
        WHEN TRIM(Main_Focus) = '' THEN NULL
        ELSE TRIM(Main_Focus)
    END AS main_focus,
    
    -- Normalisation de la région
    CASE
        WHEN Region IS NULL THEN NULL
        WHEN TRIM(Region) = '' THEN NULL
        ELSE TRIM(Region)
    END AS region,
    
    -- Conversion robuste de l'année de fondation
    CASE
        WHEN Year_Founded IS NULL THEN NULL
        WHEN TRIM(CAST(Year_Founded AS STRING)) = '' THEN NULL
        ELSE SAFE_CAST(Year_Founded AS INT64)
    END AS year_founded,
    
    -- Timestamp de chargement
    CURRENT_TIMESTAMP() AS loaded_at
FROM source