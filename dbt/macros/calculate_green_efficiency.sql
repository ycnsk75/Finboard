{% macro calculate_green_efficiency(marketcap_usd, co2_emissions_mt) %}
  CASE 
    WHEN {{ co2_emissions_mt }} IS NULL OR {{ co2_emissions_mt }} = 0 THEN NULL
    WHEN {{ marketcap_usd }} IS NULL THEN NULL
    ELSE {{ marketcap_usd }} / {{ co2_emissions_mt }}
  END
{% endmacro %}