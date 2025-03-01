WITH source AS (
    SELECT * FROM {{ source('raw', 'raw_green_stock_carbon') }}
)

SELECT
    ID AS id,
    Company AS company_name,
    Ticker AS ticker,
    CAST(Electricity_Consumption_GWh AS FLOAT64) AS electricity_consumption_gwh,
    CAST(CO2_Emissions_Mt AS FLOAT64) AS co2_emissions_mt,
    CAST(Electrical_Power_GWh AS FLOAT64) AS electrical_power_gwh,
    CURRENT_TIMESTAMP() AS loaded_at
FROM source