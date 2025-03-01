{{ config(materialized='view') }}

WITH cleaned AS (
  SELECT
    ticker,
    current_price AS price,
    marketCapitalization AS marketcap_usd,
    REGEXP_EXTRACT(_FILE_NAME, r'\d{4}-\d{2}-\d{2}') AS date
  FROM {{ source('raw', 'raw_finnhub_financials') }}
  WHERE ticker IS NOT NULL
    AND current_price IS NOT NULL
    AND marketCapitalization IS NOT NULL
)

SELECT
  ticker,
  price,
  marketcap_usd,
  CAST(date AS DATE) AS date
FROM cleaned