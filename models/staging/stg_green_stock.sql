WITH source AS (
    SELECT * FROM {{ source('raw', 'raw_green_stock') }}
)

SELECT
    ID AS id,
    Company AS company_name,
    Ticker AS ticker,
    Category AS category,
    Stock_Exchange AS stock_exchange,
    CAST(Marketcap AS FLOAT64) AS market_cap,
    ESG_Score AS esg_score,
    CAST(REGEXP_REPLACE(Revenue, '[^0-9.]', '') AS FLOAT64) AS revenue_billions,
    Main_Focus AS main_focus,
    Region AS region,
    Year_Founded AS year_founded,
    CURRENT_TIMESTAMP() AS loaded_at
FROM source
