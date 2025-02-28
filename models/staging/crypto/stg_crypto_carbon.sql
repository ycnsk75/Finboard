WITH source AS (
    SELECT * FROM `devops-practice-449210.finboard.green_crypto_carbon_mj`
)

SELECT
    Coin as crypto_name,
    Symbol as crypto_symbol,
    Type as consensus_mechanism,
    Marketcap as market_cap_usd,
    Electrical_Power as power_kw,
    Electricity_Consumption as energy_consumption_kwh,
    CO2_Emissions as carbon_emission_kg_co2
FROM source