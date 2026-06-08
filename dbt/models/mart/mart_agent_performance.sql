{{
  config(schema='mart', materialized='table')
}}

with sales as (select * from {{ ref('mart_property_sales') }})

select
    agent_name,
    agency_name,
    agent_region,
    count(*)                                      as total_transactions,
    round(avg(sale_price), 0)                    as avg_sale_price,
    round(avg(days_on_market), 1)                as avg_days_on_market,
    round(avg(price_vs_listing_pct), 2)          as avg_price_vs_listing_pct,
    min(sale_date)                               as first_sale_date,
    max(sale_date)                               as last_sale_date
from sales
where agent_name is not null
group by agent_name, agency_name, agent_region
