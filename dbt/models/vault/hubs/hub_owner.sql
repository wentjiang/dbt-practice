{{
  config(
    materialized='incremental',
    unique_key='owner_hk',
    schema='gold'
  )
}}

with source as (
    select
        owner_hk,
        owner_id,
        load_date,
        record_source
    from {{ ref('stg_owners') }}
)

select * from source

{% if is_incremental() %}
where owner_hk not in (select owner_hk from {{ this }})
{% endif %}
