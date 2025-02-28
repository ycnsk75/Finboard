with source as (
    select * from {{ source('finboard', 'green_stock_carbon_bkp') }}
),


renamed as (
    select
        Company as company_name,
        Ticker as ticker,
        Electricity_Consumption_GW as electricity_consumption_gw,
        CO2_Emissions as co2_emissions,
        Electrical_Power_GW as electrical_power_gw,
        current_timestamp() as _loaded_at
    from source
)

select * from renamed