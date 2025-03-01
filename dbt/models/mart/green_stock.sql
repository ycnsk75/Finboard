{{ config(
    materialized='incremental',
    unique_key='ticker || "-" || date'
) }}

WITH stock_financials AS (
  SELECT 
    ticker,
    price AS current_value,
    marketcap_usd,
    date,
    ROW_NUMBER() OVER (PARTITION BY ticker, date ORDER BY date DESC) AS rn  -- Deduplicate by latest per day
  FROM {{ ref('stg_finnhub_financials') }}
  WHERE {% if is_incremental() %}
    date >= (SELECT MAX(date) FROM {{ this }})
  {% endif %}
),

stock_financials_deduped AS (
  SELECT 
    ticker,
    current_value,
    marketcap_usd,
    date
  FROM stock_financials
  WHERE rn = 1
),

stocks_combined AS (
  SELECT 
    s.name,
    s.ticker,
    s.type,
    f.current_value,
    f.marketcap_usd,
    s.Stock_Exchange,
    s.Marketcap AS marketcap_raw,
    s.ESG_Score,
    s.Revenue,
    s.Main_Focus,
    s.Region,
    s.Year_Founded,
    c.Electricity_Consumption_GWh,
    c.CO2_Emissions_Mt,
    c.Electrical_Power_GWh,
    c.CO2_Emissions_Mt * 1000 / 52 AS co2_emissions_weekly_kg,
    f.date
  FROM {{ ref('stg_green_stock') }} s
  LEFT JOIN stock_financials_deduped f ON s.ticker = f.ticker
  LEFT JOIN {{ ref('stg_green_stock_carbon') }} c ON s.ticker = c.ticker
  WHERE {% if is_incremental() %}
    f.date >= (SELECT MAX(date) FROM {{ this }})
    OR f.date IS NULL  -- Include static data if no new financials
  {% endif %}
),

efficiency AS (
  SELECT 
    name,
    ticker,
    type,
    current_value,
    marketcap_usd,
    Stock_Exchange,
    marketcap_raw,
    ESG_Score,
    Revenue,
    Main_Focus,
    Region,
    Year_Founded,
    Electricity_Consumption_GWh,
    CO2_Emissions_Mt,
    Electrical_Power_GWh,
    co2_emissions_weekly_kg,
    date,
    {{ calculate_green_efficiency('marketcap_usd', 'co2_emissions_weekly_kg / 1000 * 52') }} AS raw_green_efficiency_score,
    MIN({{ calculate_green_efficiency('marketcap_usd', 'co2_emissions_weekly_kg / 1000 * 52') }}) OVER () AS min_score,
    MAX({{ calculate_green_efficiency('marketcap_usd', 'co2_emissions_weekly_kg / 1000 * 52') }}) OVER () AS max_score
  FROM stocks_combined
)

SELECT 
  name,
  ticker,
  type,
  current_value,
  marketcap_usd,
  Stock_Exchange,
  marketcap_raw,
  ESG_Score,
  Revenue,
  Main_Focus,
  Region,
  Year_Founded,
  Electricity_Consumption_GWh,
  CO2_Emissions_Mt,
  Electrical_Power_GWh,
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