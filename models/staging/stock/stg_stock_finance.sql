with source as (
    select * from {{ source('raw', 'green_stock') }}
),

renamed as (
    select
        ID as stock_id,
        Company as company_name,
        Ticker as ticker_symbol,
        Category as stock_category,
        Exchange as exchange,
        `Marketcap B_dollars` as market_cap_billion_usd,
        ESG_Score as esg_score,
        `Revenue B_dollars` as revenue_billion_usd,
        Main_Focus as main_focus,
        Region as region,
        Year_Founded as year_founded,
        current_timestamp() as loaded_at
    from source
)

select * from renamed