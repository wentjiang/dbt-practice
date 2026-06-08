{{
  config(schema='mart', materialized='table')
}}

with transactions as (
    select
        ht.transaction_hk,
        ht.transaction_id,
        std.sale_price,
        std.sale_date,
        std.settlement_date,
        std.days_on_market,
        std.sale_type,
        std.load_date,
        std.record_source
    from {{ ref('hub_transaction') }} ht
    join {{ ref('sat_transaction_details') }} std
        on ht.transaction_hk = std.transaction_hk
        and std.load_end_date is null
),

properties as (
    select
        hp.property_hk,
        hp.property_id,
        sp.address,
        sp.suburb,
        sp.postcode,
        sp.state,
        sp.bedrooms,
        sp.bathrooms,
        sp.land_size_sqm,
        sp.property_type,
        sp.year_built,
        sp.listing_price
    from {{ ref('hub_property') }} hp
    join {{ ref('sat_property_details') }} sp
        on hp.property_hk = sp.property_hk
        and sp.load_end_date is null
),

agents as (
    select ha.agent_hk, ha.agent_id, sa.full_name as agent_name, sa.agency_name, sa.region
    from {{ ref('hub_agent') }} ha
    join {{ ref('sat_agent_details') }} sa
        on ha.agent_hk = sa.agent_hk
        and sa.load_end_date is null
),

lnk_prop as (select * from {{ ref('lnk_transaction_property') }}),
lnk_agent as (select * from {{ ref('lnk_transaction_agent') }})

select
    t.transaction_id,
    t.sale_date,
    t.sale_price,
    t.sale_type,
    t.days_on_market,
    t.settlement_date,
    p.property_id,
    p.address,
    p.suburb,
    p.state,
    p.property_type,
    p.bedrooms,
    p.bathrooms,
    p.land_size_sqm,
    p.postcode,
    p.year_built,
    p.listing_price,
    round(100.0 * (t.sale_price - p.listing_price) / p.listing_price, 2) as price_vs_listing_pct,
    a.agent_name,
    a.agency_name,
    a.region as agent_region
from transactions t
join lnk_prop lp on t.transaction_hk = lp.transaction_hk
join properties p on lp.property_hk = p.property_hk
left join lnk_agent la on t.transaction_hk = la.transaction_hk
left join agents a on la.agent_hk = a.agent_hk
