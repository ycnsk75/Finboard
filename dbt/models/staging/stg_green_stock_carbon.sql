{{ config(materialized='view') }}

SELECT
  Company AS name,
  Ticker AS ticker,
  Electricity_Consumption_GWh,
  CO2_Emissions_Mt,
  Electrical_Power_GWh
FROM {{ source('raw', 'raw_green_stock_carbon') }}
WHERE Company IS NOT NULL AND Ticker IS NOT NULL