{{
  config(
    materialized='incremental',
    unique_key=['agent_hk', 'load_date'],
    schema='gold',
    post_hook="""
      UPDATE {{ this }} AS old_row
      SET load_end_date = new_row.load_date
      FROM {{ this }} AS new_row
      WHERE old_row.agent_hk = new_row.agent_hk
        AND old_row.load_date < new_row.load_date
        AND old_row.load_end_date IS NULL
        AND new_row.load_end_date IS NULL
    """
  )
}}

with source as (
    select
        agent_hk,
        full_name, email, agency_name, license_no,
        region, accreditation_tier, active_from,
        hashdiff,
        load_date,
        record_source
    from {{ ref('stg_agents') }}
),

{% if is_incremental() %}
latest as (
    select distinct on (agent_hk) agent_hk, hashdiff
    from {{ this }}
    where load_end_date is null
    order by agent_hk, load_date desc
),

new_or_changed as (
    select s.*
    from source s
    left join latest l on s.agent_hk = l.agent_hk
    where l.agent_hk is null
       or s.hashdiff != l.hashdiff
)
{% else %}
new_or_changed as (select * from source)
{% endif %}

select
    agent_hk,
    full_name, email, agency_name, license_no,
    region, accreditation_tier, active_from,
    hashdiff,
    load_date,
    cast(null as timestamp) as load_end_date,
    record_source
from new_or_changed
