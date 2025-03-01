-- models/marts/stock_mart/fact_stock_environmental.sql
WITH stock_metrics AS (
    SELECT * FROM {{ ref('int_stock_metrics') }}
)

SELECT
    id,
    company_name,
    ticker,
    electricity_consumption_gwh,
    co2_emissions_mt,
    electrical_power_gwh,
    -- Métriques d'efficacité
    revenue_per_co2,
    market_cap_per_co2,
    revenue_per_gwh,
    
    -- Score environnemental combiné (déjà calculé dans int_stock_metrics)
    combined_environmental_score,
    
    -- Conversion en échelle 0-100 pour faciliter les comparaisons
    SAFE_CAST(
        CASE 
            WHEN combined_environmental_score IS NULL THEN NULL
            ELSE combined_environmental_score * 2  -- Supposant que le score max est ~50
        END AS FLOAT64
    ) AS normalized_environmental_score,
    
    -- Efficacité carbone: tonnes CO2 par million de dollars de revenus
    CASE
        WHEN revenue_billions > 0 AND co2_emissions_mt IS NOT NULL
        THEN SAFE_CAST(co2_emissions_mt * 1000000 / (revenue_billions * 1000) AS FLOAT64)
        ELSE NULL
    END AS carbon_intensity_tonnes_per_million,
    
    -- Classement relatif d'efficacité carbone (normalisé de 0 à 100)
    CASE
        WHEN revenue_per_co2 IS NULL THEN NULL
        WHEN revenue_per_co2 = 0 THEN 0
        ELSE SAFE_CAST(
            100 * (revenue_per_co2 / NULLIF((SELECT MAX(revenue_per_co2) FROM stock_metrics), 0))
            AS FLOAT64)
    END AS carbon_efficiency_percentile,
    
    -- Catégorie de performance environnementale
    CASE
        WHEN combined_environmental_score >= 40 THEN 'Leader environnemental'
        WHEN combined_environmental_score >= 30 THEN 'Très bonne performance'
        WHEN combined_environmental_score >= 20 THEN 'Bonne performance'
        WHEN combined_environmental_score >= 10 THEN 'Performance moyenne'
        WHEN combined_environmental_score IS NOT NULL THEN 'Performance à améliorer'
        ELSE 'Non évalué'
    END AS environmental_performance_category,
    
    -- Classement par émission de carbone
    CASE
        WHEN co2_emissions_mt IS NULL THEN 'Non évalué'
        WHEN co2_emissions_mt = 0 THEN 'Zéro émission'
        WHEN co2_emissions_mt < 0.1 THEN 'Très faibles émissions'
        WHEN co2_emissions_mt < 1 THEN 'Faibles émissions'
        WHEN co2_emissions_mt < 10 THEN 'Émissions modérées'
        ELSE 'Émissions élevées'
    END AS carbon_emission_category
FROM stock_metrics