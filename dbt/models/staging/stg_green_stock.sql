{{ config(materialized='view') }}

SELECT
  Company AS name,
  Ticker AS ticker,
  Category AS type,
  Stock_Exchange,
  Marketcap,
  ESG_Score,
  Revenue,
  Main_Focus,
  Region,
  Year_Founded
FROM {{ source('raw', 'raw_green_stock') }}
WHERE Company IS NOT NULL AND Ticker IS NOT NULL