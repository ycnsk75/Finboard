-- models/staging/stg_green_stock_carbon.sql
WITH source AS (
    SELECT * FROM {{ source('raw', 'raw_green_stock_carbon') }}
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
        WHEN TRIM(Ticker) = 'Not Listed' THEN 'UNLISTED'
        ELSE TRIM(UPPER(Ticker))  -- Standardisation en majuscules
    END AS ticker,
    
    -- Conversion robuste de la consommation d'électricité
    CASE
        WHEN Electricity_Consumption_GWh IS NULL THEN NULL
        WHEN TRIM(CAST(Electricity_Consumption_GWh AS STRING)) = '' THEN NULL
        WHEN TRIM(CAST(Electricity_Consumption_GWh AS STRING)) = '0' THEN 0
        -- Gestion de la notation scientifique
        WHEN REGEXP_CONTAINS(CAST(Electricity_Consumption_GWh AS STRING), r'^\d+\.\d+e[+-]\d+$') THEN 
            SAFE_CAST(Electricity_Consumption_GWh AS FLOAT64)
        -- Gestion d'autres formats potentiels
        WHEN REGEXP_CONTAINS(CAST(Electricity_Consumption_GWh AS STRING), r'[^0-9.\-e]') THEN
            SAFE_CAST(REGEXP_REPLACE(CAST(Electricity_Consumption_GWh AS STRING), r'[^0-9.\-e]', '') AS FLOAT64)
        ELSE SAFE_CAST(Electricity_Consumption_GWh AS FLOAT64)
    END AS electricity_consumption_gwh,
    
    -- Conversion robuste des émissions de CO2
    CASE
        WHEN CO2_Emissions_Mt IS NULL THEN NULL
        WHEN TRIM(CAST(CO2_Emissions_Mt AS STRING)) = '' THEN NULL
        WHEN TRIM(CAST(CO2_Emissions_Mt AS STRING)) = '0' THEN 0
        -- Gestion de la notation scientifique
        WHEN REGEXP_CONTAINS(CAST(CO2_Emissions_Mt AS STRING), r'^\d+\.\d+e[+-]\d+$') THEN 
            SAFE_CAST(CO2_Emissions_Mt AS FLOAT64)
        -- Gestion d'autres formats potentiels
        WHEN REGEXP_CONTAINS(CAST(CO2_Emissions_Mt AS STRING), r'[^0-9.\-e]') THEN
            SAFE_CAST(REGEXP_REPLACE(CAST(CO2_Emissions_Mt AS STRING), r'[^0-9.\-e]', '') AS FLOAT64)
        ELSE SAFE_CAST(CO2_Emissions_Mt AS FLOAT64)
    END AS co2_emissions_mt,
    
    -- Conversion robuste de la puissance électrique
    CASE
        WHEN Electrical_Power_GWh IS NULL THEN NULL
        WHEN TRIM(CAST(Electrical_Power_GWh AS STRING)) = '' THEN NULL
        WHEN TRIM(CAST(Electrical_Power_GWh AS STRING)) = '0' THEN 0
        -- Gestion de la notation scientifique
        WHEN REGEXP_CONTAINS(CAST(Electrical_Power_GWh AS STRING), r'^\d+\.\d+e[+-]\d+$') THEN 
            SAFE_CAST(Electrical_Power_GWh AS FLOAT64)
        -- Gestion d'autres formats potentiels
        WHEN REGEXP_CONTAINS(CAST(Electrical_Power_GWh AS STRING), r'[^0-9.\-e]') THEN
            SAFE_CAST(REGEXP_REPLACE(CAST(Electrical_Power_GWh AS STRING), r'[^0-9.\-e]', '') AS FLOAT64)
        ELSE SAFE_CAST(Electrical_Power_GWh AS FLOAT64)
    END AS electrical_power_gwh,
    
    -- Timestamp de chargement
    CURRENT_TIMESTAMP() AS loaded_at
FROM source