{{ config(materialized='view') }}

WITH cleaned AS (
  SELECT
    coin,
    price_usd AS price,
    REGEXP_EXTRACT(_FILE_NAME, r'\d{4}-\d{2}-\d{2}') AS date
  FROM {{ source('raw', 'raw_coincap_prices') }}
  WHERE coin IS NOT NULL
    AND price_usd IS NOT NULL
)

SELECT
  coin,
  price,
  CAST(date AS DATE) AS date
FROM cleaned