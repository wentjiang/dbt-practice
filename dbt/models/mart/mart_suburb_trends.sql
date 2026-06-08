{{
  config(schema='mart', materialized='table')
}}

with sales as (select * from {{ ref('mart_property_sales') }})

select
    suburb,
    state,
    date_trunc('month', sale_date)               as sale_month,
    count(*)                                     as transaction_count,
    round(avg(sale_price), 0)                   as avg_sale_price,
    round(percentile_cont(0.5) within group
          (order by sale_price), 0)             as median_sale_price,
    round(avg(days_on_market), 1)               as avg_days_on_market
from sales
group by suburb, state, date_trunc('month', sale_date)
