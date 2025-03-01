WITH source AS (
    SELECT * FROM {{ source('raw', 'raw_coincap_prices') }}
)

SELECT
    coin,
    price_usd,
    CURRENT_TIMESTAMP() AS loaded_at
FROM source