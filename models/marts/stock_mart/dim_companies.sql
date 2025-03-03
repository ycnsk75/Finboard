-- models/marts/stock_mart/dim_companies.sql
WITH stock_metrics AS (
    SELECT * FROM {{ ref('int_stock_metrics') }}
)

SELECT
    id,
    company_name,
    ticker,
    category,
    stock_exchange,
    market_cap,
    esg_score,
    revenue_billions,
    main_focus,
    region,
    year_founded,
    current_price,
    -- Données additionnelles pour des analyses plus riches
    EXTRACT(YEAR FROM CURRENT_DATE()) - year_founded AS company_age,
    -- Classification par taille d'entreprise
    CASE
        WHEN market_cap IS NULL THEN 'Non classifiée'
        WHEN market_cap >= 10000 THEN 'Large Cap (>10B)'
        WHEN market_cap >= 2000 THEN 'Mid Cap (2-10B)'
        WHEN market_cap > 0 THEN 'Small Cap (<2B)'
        ELSE 'Non classifiée'
    END AS company_size,
    -- Classification par score ESG
    CASE
        WHEN esg_score IS NULL THEN 'Non évalué'
        WHEN esg_score >= 40 THEN 'ESG Excellent'
        WHEN esg_score >= 35 THEN 'ESG Très bon'
        WHEN esg_score >= 30 THEN 'ESG Bon'
        WHEN esg_score >= 25 THEN 'ESG Moyen'
        ELSE 'ESG À améliorer'
    END AS esg_category,
    -- Classification par région
    CASE
        WHEN region = 'North America' THEN 'Amérique du Nord'
        WHEN region = 'Europe' THEN 'Europe'
        WHEN region = 'Asia' THEN 'Asie'
        WHEN region LIKE '%America%' AND region != 'North America' THEN 'Amérique Latine'
        WHEN region LIKE '%Africa%' THEN 'Afrique'
        WHEN region LIKE '%Pacific%' OR region = 'Australia' OR region = 'Oceania' THEN 'Asie-Pacifique'
        ELSE region
    END AS region_group
FROM stock_metrics