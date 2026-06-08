{{
  config(schema='mart', materialized='table')
}}

with owners as (
    select ho.owner_hk, ho.owner_id, so.full_name, so.suburb, so.state, so.investor_flag
    from {{ ref('hub_owner') }} ho
    join {{ ref('sat_owner_details') }} so
        on ho.owner_hk = so.owner_hk
        and so.load_end_date is null
),

buyers as (
    select lb.owner_hk, count(*) as buy_count
    from {{ ref('lnk_transaction_buyer') }} lb
    group by lb.owner_hk
),

sellers as (
    select ls.owner_hk, count(*) as sell_count
    from {{ ref('lnk_transaction_seller') }} ls
    group by ls.owner_hk
)

select
    o.owner_id,
    o.full_name,
    o.suburb,
    o.state,
    o.investor_flag,
    coalesce(b.buy_count, 0)      as total_purchases,
    coalesce(s.sell_count, 0)     as total_sales,
    coalesce(b.buy_count, 0)
      - coalesce(s.sell_count, 0) as net_properties_held
from owners o
left join buyers b on o.owner_hk = b.owner_hk
left join sellers s on o.owner_hk = s.owner_hk
