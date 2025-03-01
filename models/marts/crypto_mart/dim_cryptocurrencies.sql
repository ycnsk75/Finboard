WITH crypto_metrics AS (
    SELECT * FROM {{ ref('int_crypto_metrics') }}
)

SELECT
    coin_name,
    symbol,
    price_usd,
    green_price,
    percent_change,
    market_cap,
    environmental_impact_category
FROM crypto_metrics
