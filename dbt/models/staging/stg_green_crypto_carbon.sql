{{ config(materialized='view') }}

SELECT
  Coin AS name,
  Symbol AS ticker,
  Type AS type,
  Marketcap,
  Electrical_Power_GW,
  Electricity_Consumption_GW,
  CO2_Emissions_Mt
FROM {{ source('raw', 'raw_green_crypto_carbon') }}
WHERE Coin IS NOT NULL