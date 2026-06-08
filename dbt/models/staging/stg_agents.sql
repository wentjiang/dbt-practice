with source as (
    select * from {{ source('bronze', 'src_crm_agents') }}
),

staged as (
    select
        agent_id,
        full_name,
        email,
        agency_name,
        license_no,
        region,
        accreditation_tier,
        cast(active_from as date)        as active_from,
        cast(_loaded_at as timestamp)    as load_date,
        'src_crm.agents'                 as record_source,

        md5(cast(agent_id as text))      as agent_hk,
        md5(concat_ws('||',
            coalesce(full_name,''),
            coalesce(email,''),
            coalesce(agency_name,''),
            coalesce(license_no,''),
            coalesce(region,''),
            coalesce(accreditation_tier,''),
            cast(active_from as text)
        ))                               as hashdiff
    from source
    where agent_id is not null
)

select * from staged
