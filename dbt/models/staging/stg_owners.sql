with source as (
    select * from {{ source('bronze', 'src_crm_owners') }}
),

staged as (
    select
        owner_id,
        full_name,
        email,
        phone,
        suburb,
        state,
        cast(registration_date as date)  as registration_date,
        cast(investor_flag as boolean)   as investor_flag,
        cast(_loaded_at as timestamp)    as load_date,
        'src_crm.owners'                 as record_source,

        md5(cast(owner_id as text))      as owner_hk,
        md5(concat_ws('||',
            coalesce(full_name,''),
            coalesce(email,''),
            coalesce(phone,''),
            coalesce(suburb,''),
            coalesce(state,''),
            cast(registration_date as text),
            cast(investor_flag as text)
        ))                               as hashdiff
    from source
    where owner_id is not null
)

select * from staged
