with source as (
    select * from {{ source('finboard', 'green_stock_bkp') }}
),

renamed as (
    select
        ID as stock_id,
        Company as company_name,
        Ticker as ticker_symbol,
        Category as stock_category,
        Stock_Exchange as exchange,
        Marketcap_B_dollar as market_cap_billion_usd,
        ESG_Score as esg_score,
        Revenue_B_dollar as revenue_billion_usd,
        Main_Focus as main_focus,
        Region as region,
        Year_Founded as year_founded,
        current_timestamp() as loaded_at
    from source
)

select * from renamed