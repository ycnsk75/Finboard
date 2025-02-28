with

crypto_finance as (
    select * from {{ ref('stg_crypto_finance') }}
),

crypto_carbon as (
    select * from {{ ref('stg_crypto_carbon') }}
),

joined as (
    select
        cf.crypto_name,
        cf.crypto_symbol,
        cf.price_usd,
        cf.price_change,
        cf.percent_change,
        cf.high_24h,
        cf.low_24h,
        cf.open_24h,
        cf.previous_close,
        cf.timestamp,
        cc.consensus_mechanism,
        cc.market_cap_usd,
        cc.power_kw,
        cc.energy_consumption_kwh,
        cc.carbon_emission_kg_co2,
        -- Calcul de métriques additionnelles
        cc.carbon_emission_kg_co2 / NULLIF(cc.market_cap_usd, 0) * 1000000000 as carbon_intensity_per_billion_usd,
        cc.energy_consumption_kwh / NULLIF(cc.market_cap_usd, 0) * 1000000000 as energy_intensity_per_billion_usd
    from crypto_finance cf
    left join crypto_carbon cc
        on cf.crypto_symbol = cc.crypto_symbol
)

select * from joined