{{
  config(
    materialized='incremental',
    unique_key=['property_hk', 'load_date'],
    schema='gold',
    post_hook="""
      UPDATE {{ this }} AS old_row
      SET load_end_date = new_row.load_date
      FROM {{ this }} AS new_row
      WHERE old_row.property_hk = new_row.property_hk
        AND old_row.load_date < new_row.load_date
        AND old_row.load_end_date IS NULL
        AND new_row.load_end_date IS NULL
    """
  )
}}

with source as (
    select
        property_hk,
        address, suburb, postcode, state,
        bedrooms, bathrooms, land_size_sqm,
        property_type, year_built, listing_price,
        hashdiff,
        load_date,
        record_source
    from {{ ref('stg_properties') }}
),

{% if is_incremental() %}
latest as (
    select distinct on (property_hk) property_hk, hashdiff
    from {{ this }}
    where load_end_date is null
    order by property_hk, load_date desc
),

new_or_changed as (
    select s.*
    from source s
    left join latest l on s.property_hk = l.property_hk
    where l.property_hk is null
       or s.hashdiff != l.hashdiff
)
{% else %}
new_or_changed as (select * from source)
{% endif %}

select
    property_hk,
    address, suburb, postcode, state,
    bedrooms, bathrooms, land_size_sqm,
    property_type, year_built, listing_price,
    hashdiff,
    load_date,
    cast(null as timestamp) as load_end_date,
    record_source
from new_or_changed
