WITH finnhub_financials AS (
    SELECT * FROM {{ ref('stg_finnhub_financials') }}
),

green_stock AS (
    SELECT * FROM {{ ref('stg_green_stock') }}
),

stock_carbon AS (
    SELECT * FROM {{ ref('stg_green_stock_carbon') }}
)

SELECT
    gs.id,
    gs.company_name,
    gs.ticker,
    gs.category,
    gs.stock_exchange,
    COALESCE(gs.market_cap, ff.market_cap) AS market_cap,
    gs.esg_score,
    gs.revenue_billions,
    gs.main_focus,
    gs.region,
    gs.year_founded,
    sc.electricity_consumption_gwh,
    sc.co2_emissions_mt,
    sc.electrical_power_gwh,
    ff.current_price,
    -- Calcul des métriques environnementales
    CASE
        WHEN sc.co2_emissions_mt > 0 AND gs.revenue_billions > 0
        THEN gs.revenue_billions / sc.co2_emissions_mt
        ELSE NULL
    END AS revenue_per_co2,
    CASE
        WHEN sc.co2_emissions_mt > 0 AND COALESCE(gs.market_cap, ff.market_cap) > 0
        THEN COALESCE(gs.market_cap, ff.market_cap) / sc.co2_emissions_mt
        ELSE NULL
    END AS market_cap_per_co2,
    -- Efficacité énergétique
    CASE
        WHEN sc.electricity_consumption_gwh > 0 AND gs.revenue_billions > 0
        THEN gs.revenue_billions / sc.electricity_consumption_gwh
        ELSE NULL
    END AS revenue_per_gwh,
    -- Score environnemental combiné (ESG + efficacité carbone)
    CASE
        WHEN gs.esg_score IS NOT NULL AND sc.co2_emissions_mt IS NOT NULL
        THEN gs.esg_score * (1 - LEAST(sc.co2_emissions_mt / 10, 0.5))
        ELSE gs.esg_score
    END AS combined_environmental_score
FROM green_stock gs
LEFT JOIN stock_carbon sc ON gs.id = sc.id
LEFT JOIN finnhub_financials ff ON gs.ticker = ff.ticker