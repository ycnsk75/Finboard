-- models/marts/green_finance/environmental_impact_comparison.sql
WITH green_overview AS (
    SELECT * FROM {{ ref('green_investment_overview') }}
)

-- Agrégation par classe d'actif
SELECT
    asset_type,
    COUNT(*) AS total_assets,
    SAFE_CAST(AVG(co2_emissions_mt) AS FLOAT64) AS avg_co2_emissions,
    SAFE_CAST(SUM(co2_emissions_mt) AS FLOAT64) AS total_co2_emissions,
    SAFE_CAST(AVG(green_efficiency) AS FLOAT64) AS avg_efficiency,
    SAFE_CAST(AVG(environmental_score) AS FLOAT64) AS avg_environmental_score,
    SAFE_CAST(SUM(market_cap) AS FLOAT64) AS total_market_cap,
    SAFE_CAST(SUM(COALESCE(co2_emissions_mt, 0)) / NULLIF(SUM(COALESCE(market_cap, 0)), 0) * 1000000 AS FLOAT64) AS carbon_intensity_per_million_usd,
    SAFE_CAST(SUM(COALESCE(energy_consumption, 0)) / NULLIF(SUM(COALESCE(market_cap, 0)), 0) * 1000000 AS FLOAT64) AS energy_intensity_per_million_usd
FROM green_overview
GROUP BY asset_type

UNION ALL

-- Agrégation par sous-type d'actif
SELECT
    asset_subtype AS asset_type,
    COUNT(*) AS total_assets,
    SAFE_CAST(AVG(co2_emissions_mt) AS FLOAT64) AS avg_co2_emissions,
    SAFE_CAST(SUM(co2_emissions_mt) AS FLOAT64) AS total_co2_emissions,
    SAFE_CAST(AVG(green_efficiency) AS FLOAT64) AS avg_efficiency,
    SAFE_CAST(AVG(environmental_score) AS FLOAT64) AS avg_environmental_score,
    SAFE_CAST(SUM(market_cap) AS FLOAT64) AS total_market_cap,
    SAFE_CAST(SUM(COALESCE(co2_emissions_mt, 0)) / NULLIF(SUM(COALESCE(market_cap, 0)), 0) * 1000000 AS FLOAT64) AS carbon_intensity_per_million_usd,
    SAFE_CAST(SUM(COALESCE(energy_consumption, 0)) / NULLIF(SUM(COALESCE(market_cap, 0)), 0) * 1000000 AS FLOAT64) AS energy_intensity_per_million_usd
FROM green_overview
GROUP BY asset_subtype

UNION ALL

-- Statistiques globales pour toutes les classes d'actifs
SELECT
    'Tous actifs' AS asset_type,
    COUNT(*) AS total_assets,
    SAFE_CAST(AVG(co2_emissions_mt) AS FLOAT64) AS avg_co2_emissions,
    SAFE_CAST(SUM(co2_emissions_mt) AS FLOAT64) AS total_co2_emissions,
    SAFE_CAST(AVG(green_efficiency) AS FLOAT64) AS avg_efficiency,
    SAFE_CAST(AVG(environmental_score) AS FLOAT64) AS avg_environmental_score,
    SAFE_CAST(SUM(market_cap) AS FLOAT64) AS total_market_cap,
    SAFE_CAST(SUM(COALESCE(co2_emissions_mt, 0)) / NULLIF(SUM(COALESCE(market_cap, 0)), 0) * 1000000 AS FLOAT64) AS carbon_intensity_per_million_usd,
    SAFE_CAST(SUM(COALESCE(energy_consumption, 0)) / NULLIF(SUM(COALESCE(market_cap, 0)), 0) * 1000000 AS FLOAT64) AS energy_intensity_per_million_usd
FROM green_overview