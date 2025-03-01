-- models/staging/stg_green_crypto_carbon.sql
WITH source AS (
    SELECT * FROM {{ source('raw', 'raw_green_crypto_carbon') }}
)

SELECT
    Coin AS coin_name,
    Symbol AS symbol,
    Type AS asset_type,
    -- Gestion robuste de la capitalisation boursière avec différents formats
    CASE
        WHEN Marketcap IS NULL THEN NULL
        WHEN TRIM(Marketcap) = '' THEN NULL
        WHEN TRIM(Marketcap) = '0' THEN 0
        -- Gestion des valeurs avec format $X,XXX,XXX.XXX
        WHEN STARTS_WITH(Marketcap, '$') THEN 
            SAFE_CAST(REGEXP_REPLACE(REGEXP_REPLACE(Marketcap, r'[$,]', ''), r'[^0-9.]', '') AS FLOAT64)
        -- Gestion des valeurs numériques simples
        ELSE 
            SAFE_CAST(REGEXP_REPLACE(Marketcap, r'[^0-9.]', '') AS FLOAT64)
    END AS market_cap,
    
    -- Gestion sécurisée de la puissance électrique
    SAFE_CAST(
        CASE
            WHEN Electrical_Power_GW IS NULL OR TRIM(CAST(Electrical_Power_GW AS STRING)) = '' THEN NULL
            ELSE Electrical_Power_GW
        END AS FLOAT64
    ) AS electrical_power_gw,
    
    -- Gestion sécurisée de la consommation d'électricité
    SAFE_CAST(
        CASE
            WHEN Electricity_Consumption_GW IS NULL OR TRIM(CAST(Electricity_Consumption_GW AS STRING)) = '' THEN NULL
            -- Gestion des valeurs scientifiques (ex: 3.6e-05)
            WHEN REGEXP_CONTAINS(CAST(Electricity_Consumption_GW AS STRING), r'^\d+\.\d+e[+-]\d+$') THEN Electricity_Consumption_GW
            ELSE Electricity_Consumption_GW
        END AS FLOAT64
    ) AS electricity_consumption_gw,
    
    -- Gestion sécurisée des émissions de CO2
    SAFE_CAST(
        CASE
            WHEN CO2_Emissions_Mt IS NULL OR TRIM(CAST(CO2_Emissions_Mt AS STRING)) = '' THEN NULL
            -- Gestion des valeurs scientifiques (ex: 1.4e-08)
            WHEN REGEXP_CONTAINS(CAST(CO2_Emissions_Mt AS STRING), r'^\d+\.\d+e[+-]\d+$') THEN CO2_Emissions_Mt
            ELSE CO2_Emissions_Mt
        END AS FLOAT64
    ) AS co2_emissions_mt,
    
    CURRENT_TIMESTAMP() AS loaded_at
FROM source