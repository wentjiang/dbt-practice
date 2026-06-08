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
        seller_hk     as owner_hk,
        load_date,
        record_source
    from {{ ref('stg_transactions') }}
)

select * from source

{% if is_incremental() %}
where transaction_hk not in (select transaction_hk from {{ this }})
{% endif %}
