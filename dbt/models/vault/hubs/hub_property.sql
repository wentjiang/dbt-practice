{{
  config(
    materialized='incremental',
    unique_key='property_hk',
    schema='gold'
  )
}}

with source as (
    select
        property_hk,
        property_id,
        load_date,
        record_source
    from {{ ref('stg_properties') }}
)

select * from source

{% if is_incremental() %}
where property_hk not in (select property_hk from {{ this }})
{% endif %}
