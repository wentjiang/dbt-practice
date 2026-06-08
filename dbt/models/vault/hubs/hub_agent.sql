{{
  config(
    materialized='incremental',
    unique_key='agent_hk',
    schema='gold'
  )
}}

with source as (
    select
        agent_hk,
        agent_id,
        load_date,
        record_source
    from {{ ref('stg_agents') }}
)

select * from source

{% if is_incremental() %}
where agent_hk not in (select agent_hk from {{ this }})
{% endif %}
