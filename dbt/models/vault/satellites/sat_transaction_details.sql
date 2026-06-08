{{
  config(
    materialized='incremental',
    unique_key='transaction_hk',
    schema='gold'
  )
}}

with source as (
    select
        transaction_hk,
        sale_price, sale_date, settlement_date,
        days_on_market, sale_type,
        hashdiff,
        load_date,
        record_source
    from {{ ref('stg_transactions') }}
)

select
    transaction_hk,
    sale_price, sale_date, settlement_date,
    days_on_market, sale_type,
    hashdiff,
    load_date,
    cast(null as timestamp) as load_end_date,
    record_source
from source

{% if is_incremental() %}
where transaction_hk not in (select transaction_hk from {{ this }})
{% endif %}
