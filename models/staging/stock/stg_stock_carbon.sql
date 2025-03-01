with source as (
    select * from {{ source('finboard', 'raw_green_stock_carbon') }}
),


renamed as (
    select
        Company as company_name,
        Ticker as ticker,
        Electricity_Consumption_GWh as electricity_consumption_gw,
        CO2_Emissions_Mt as co2_emissions,
        Electrical_Power_GWh as electrical_power_gw,
        current_timestamp() as _loaded_at
    from source
)

select * from renamed