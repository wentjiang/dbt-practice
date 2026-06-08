{{
  config(
    materialized='incremental',
    unique_key=['owner_hk', 'load_date'],
    schema='gold',
    post_hook="""
      UPDATE {{ this }} AS old_row
      SET load_end_date = new_row.load_date
      FROM {{ this }} AS new_row
      WHERE old_row.owner_hk = new_row.owner_hk
        AND old_row.load_date < new_row.load_date
        AND old_row.load_end_date IS NULL
        AND new_row.load_end_date IS NULL
    """
  )
}}

with source as (
    select
        owner_hk,
        full_name, email, phone, suburb, state,
        registration_date, investor_flag,
        hashdiff,
        load_date,
        record_source
    from {{ ref('stg_owners') }}
),

{% if is_incremental() %}
latest as (
    select owner_hk, hashdiff
    from {{ this }}
    where load_end_date is null
),

new_or_changed as (
    select s.*
    from source s
    left join latest l on s.owner_hk = l.owner_hk
    where l.owner_hk is null
       or s.hashdiff != l.hashdiff
)
{% else %}
new_or_changed as (select * from source)
{% endif %}

select
    owner_hk,
    full_name, email, phone, suburb, state,
    registration_date, investor_flag,
    hashdiff,
    load_date,
    cast(null as timestamp) as load_end_date,
    record_source
from new_or_changed
