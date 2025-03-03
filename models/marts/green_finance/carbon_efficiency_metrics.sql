-- models/marts/green_finance/carbon_efficiency_metrics.sql
WITH green_overview AS (
    SELECT * FROM {{ ref('green_investment_overview') }}
),

-- Calcul des statistiques par type d'actif pour les normalisations
asset_type_stats AS (
    SELECT
        asset_type,
        AVG(green_efficiency) AS avg_efficiency,
        MAX(green_efficiency) AS max_efficiency,
        MIN(green_efficiency) AS min_efficiency,
--        PERCENTILE_CONT(green_efficiency, 0.5) OVER (PARTITION BY asset_type) AS median_efficiency,
        APPROX_QUANTILES(green_efficiency, 100)[OFFSET(50)] AS median_efficiency,
        STDDEV(green_efficiency) AS stddev_efficiency,
        COUNT(*) AS count_assets
    FROM green_overview
    WHERE green_efficiency IS NOT NULL
    GROUP BY asset_type
)

SELECT
    go.asset_type,
    go.asset_name,
    go.asset_symbol,
    go.current_price,
    go.market_cap,
    go.co2_emissions_mt,
    go.energy_consumption,
    go.green_efficiency,
    go.environmental_score,
    go.environmental_category,
    
    -- Ratio d'efficacité normalisé par rapport à la moyenne de la catégorie
    SAFE_CAST(
        CASE 
            WHEN ats.avg_efficiency IS NULL OR ats.avg_efficiency = 0 THEN NULL
            ELSE go.green_efficiency / ats.avg_efficiency 
        END
        AS FLOAT64
    ) AS relative_efficiency,
    
    -- Score Z d'efficacité (combien d'écarts types au-dessus/en-dessous de la moyenne)
    SAFE_CAST(
        CASE 
            WHEN ats.stddev_efficiency IS NULL OR ats.stddev_efficiency = 0 THEN NULL
            ELSE (go.green_efficiency - ats.avg_efficiency) / ats.stddev_efficiency 
        END
        AS FLOAT64
    ) AS efficiency_z_score,
    
    -- Normalisation sur une échelle de 0 à 100
    SAFE_CAST(
        CASE 
            WHEN ats.max_efficiency IS NULL OR ats.max_efficiency = ats.min_efficiency THEN NULL
            ELSE 100 * (go.green_efficiency - ats.min_efficiency) / (ats.max_efficiency - ats.min_efficiency)
        END
        AS FLOAT64
    ) AS efficiency_percentile,
    
    -- Classement au sein de sa catégorie
    ROW_NUMBER() OVER (PARTITION BY go.asset_type ORDER BY go.green_efficiency DESC) AS efficiency_rank,
    
    -- Classement en percentile dans sa catégorie
    SAFE_CAST(
        CAST(ROW_NUMBER() OVER (PARTITION BY go.asset_type ORDER BY go.green_efficiency DESC) AS FLOAT64) / 
        NULLIF(ats.count_assets, 0) * 100
        AS FLOAT64
    ) AS efficiency_rank_percentile,
    
    -- Catégorie d'investissement vert
    CASE
        WHEN go.co2_emissions_mt IS NULL THEN 'Données insuffisantes'
        WHEN go.co2_emissions_mt = 0 THEN 'Impact zéro'
        WHEN go.green_efficiency > ats.avg_efficiency * 2 THEN 'Très efficace'
        WHEN go.green_efficiency > ats.avg_efficiency THEN 'Efficace'
        WHEN go.green_efficiency > 0 THEN 'Moyenne'
        ELSE 'Inefficace'
    END AS green_investment_category,
    
    -- Intensité carbone (CO2 par million de capitalisation)
    SAFE_CAST(
        CASE
            WHEN go.market_cap IS NULL OR go.market_cap = 0 THEN NULL
            ELSE go.co2_emissions_mt / (go.market_cap / 1000000)
        END
        AS FLOAT64
    ) AS carbon_intensity_per_million,
    
    -- Calcul du retour sur investissement écologique (capitalisation/émissions)
    SAFE_CAST(
        CASE
            WHEN go.co2_emissions_mt IS NULL OR go.co2_emissions_mt = 0 THEN NULL
            ELSE go.market_cap / (go.co2_emissions_mt * 1000000)  -- Valeur en $ par tonne de CO2
        END
        AS FLOAT64
    ) AS eco_roi_usd_per_ton_co2,
    
    -- Données de contexte
    go.asset_subtype,
    go.estimated_esg,
    go.region,
    go.year_founded,
    go.main_focus,
    CURRENT_TIMESTAMP() AS analysis_timestamp
FROM green_overview go
LEFT JOIN asset_type_stats ats ON go.asset_type = ats.asset_type
WHERE go.co2_emissions_mt IS NOT NULL