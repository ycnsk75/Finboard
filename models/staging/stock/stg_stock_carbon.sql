with source as (
    select * from {{ source('raw', 'green_stock_carbon') }}
),

renamed as (
    select
        ID as stock_id,
        Company as company_name,
        Ticker as ticker_symbol,
        Electricity_Consumption as energy_consumption,
        CO2_Emissions as carbon_emissions,
        Electrical_Power as electrical_power,
        current_timestamp() as loaded_at
    from source
)

select * from renamed