WITH source AS (
    SELECT * FROM `devops-practice-449210.finboard.green_crypto_bkp`
)

SELECT
    Coin as crypto_name,
    Symbol as crypto_symbol,
    Price as price_usd,
    Change as price_change,
    Percent_Change as percent_change,
    High_24h as high_24h,
    Low_24h as low_24h,
    Open_24h as open_24h,
    Previous_Close as previous_close,
    CAST(Timestamp as TIMESTAMP) as timestamp
FROM source 