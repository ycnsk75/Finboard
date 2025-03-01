WITH source AS (
    SELECT * FROM {{ source('raw', 'raw_finnhub_financials') }}
)

SELECT
    ticker,
    current_price,
    CAST(marketCapitalization AS FLOAT64) AS market_cap,
    CURRENT_TIMESTAMP() AS loaded_at
FROM source