{{ config(materialized='view') }}

SELECT
  Coin AS name,
  Symbol AS ticker,
  Price AS current_value,
  Change,
  Percent_Change,
  High_24h,
  Low_24h,
  Open_24h,
  Previous_Close_24h,
  Timestamp
FROM {{ source('raw', 'raw_green_crypto') }}
WHERE Coin IS NOT NULL