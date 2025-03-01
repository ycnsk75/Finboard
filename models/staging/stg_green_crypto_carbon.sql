WITH source AS (
    SELECT * FROM {{ source('raw', 'raw_green_crypto_carbon') }}
)

SELECT
    Coin AS coin_name,
    Symbol AS symbol,
    Type AS asset_type,
    CAST(REGEXP_REPLACE(Marketcap, '[^0-9.]', '') AS FLOAT64) AS market_cap,
    CAST(Electrical_Power_GW AS FLOAT64) AS electrical_power_gw,
    CAST(Electricity_Consumption_GW AS FLOAT64) AS electricity_consumption_gw,
    CAST(CO2_Emissions_Mt AS FLOAT64) AS co2_emissions_mt,
    CURRENT_TIMESTAMP() AS loaded_at
FROM source
