{{ config(
    materialized='incremental',
    unique_key='ticker || "-" || date'
) }}

WITH crypto_prices AS (
  SELECT 
    coin,
    price AS current_value,
    date,
    ROW_NUMBER() OVER (PARTITION BY coin, date ORDER BY date DESC) AS rn  -- Deduplicate by latest per day
  FROM {{ ref('stg_coincap_prices') }}
  WHERE {% if is_incremental() %}
    date >= (SELECT MAX(date) FROM {{ this }})
  {% endif %}
),

crypto_prices_deduped AS (
  SELECT 
    coin,
    current_value,
    date
  FROM crypto_prices
  WHERE rn = 1
),

crypto_combined AS (
  SELECT 
    c.name,
    c.ticker,
    c.current_value AS original_price,
    p.current_value,
    c.Change,
    c.Percent_Change,
    c.High_24h,
    c.Low_24h,
    c.Open_24h,
    c.Previous_Close_24h,
    c.Timestamp,
    cc.type,
    cc.Marketcap AS marketcap_raw,
    cc.Electrical_Power_GW,
    cc.Electricity_Consumption_GW,
    cc.CO2_Emissions_Mt,
    cc.CO2_Emissions_Mt * 1000 / 52 AS co2_emissions_weekly_kg,
    p.date
  FROM {{ ref('stg_green_crypto') }} c
  LEFT JOIN crypto_prices_deduped p ON c.name = p.coin
  LEFT JOIN {{ ref('stg_green_crypto_carbon') }} cc ON c.name = cc.name
  WHERE {% if is_incremental() %}
    p.date >= (SELECT MAX(date) FROM {{ this }})
    OR p.date IS NULL  -- Include static data if no new prices
  {% endif %}
),

efficiency AS (
  SELECT 
    name,
    ticker,
    original_price,
    current_value,
    Change,
    Percent_Change,
    High_24h,
    Low_24h,
    Open_24h,
    Previous_Close_24h,
    Timestamp,
    type,
    marketcap_raw,
    Electrical_Power_GW,
    Electricity_Consumption_GW,
    CO2_Emissions_Mt,
    co2_emissions_weekly_kg,
    date,
    CAST(REGEXP_REPLACE(marketcap_raw, '[^0-9.]', '') AS FLOAT64) AS marketcap_usd,
    {{ calculate_green_efficiency("CAST(REGEXP_REPLACE(marketcap_raw, '[^0-9.]', '') AS FLOAT64)", 'co2_emissions_weekly_kg / 1000 * 52') }} AS raw_green_efficiency_score,
    MIN({{ calculate_green_efficiency("CAST(REGEXP_REPLACE(marketcap_raw, '[^0-9.]', '') AS FLOAT64)", 'co2_emissions_weekly_kg / 1000 * 52') }}) OVER () AS min_score,
    MAX({{ calculate_green_efficiency("CAST(REGEXP_REPLACE(marketcap_raw, '[^0-9.]', '') AS FLOAT64)", 'co2_emissions_weekly_kg / 1000 * 52') }}) OVER () AS max_score
  FROM crypto_combined
)

SELECT 
  name,
  ticker,
  original_price,
  current_value,
  Change,
  Percent_Change,
  High_24h,
  Low_24h,
  Open_24h,
  Previous_Close_24h,
  Timestamp,
  type,
  marketcap_raw,
  marketcap_usd,
  Electrical_Power_GW,
  Electricity_Consumption_GW,
  CO2_Emissions_Mt,
  co2_emissions_weekly_kg,
  date,
  CASE 
    WHEN max_score = min_score THEN 100
    WHEN raw_green_efficiency_score IS NULL THEN NULL
    ELSE 100 * (raw_green_efficiency_score - min_score) / (max_score - min_score)
  END AS green_efficiency_score,
  CASE 
    WHEN max_score = min_score THEN 'High'
    WHEN raw_green_efficiency_score IS NULL THEN NULL
    WHEN 100 * (raw_green_efficiency_score - min_score) / (max_score - min_score) > 66 THEN 'High'
    WHEN 100 * (raw_green_efficiency_score - min_score) / (max_score - min_score) >= 33 THEN 'Medium'
    ELSE 'Low'
  END AS efficiency_rating
FROM efficiency
{% if is_incremental() %}
WHERE date > (SELECT MAX(date) FROM {{ this }})
   OR date IS NULL  -- Include static data
{% endif %}