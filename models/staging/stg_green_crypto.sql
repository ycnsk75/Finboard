WITH source AS (
    SELECT * FROM {{ source('raw', 'raw_green_crypto') }}
)

SELECT
    Coin AS coin_name,
    Symbol AS symbol,
    Price AS price,
    Change AS price_change,
    Percent_Change AS percent_change,
    High_24h AS high_24h,
    Low_24h AS low_24h,
    Open_24h AS open_24h,
    Previous_Close_24h AS previous_close_24h,
    Timestamp AS timestamp,
    CURRENT_TIMESTAMP() AS loaded_at
FROM source
