with source as (
    select * from {{ source('bronze', 'property_raw') }}
),

deduped as (
    select distinct on (property_id)
        property_id,
        address,
        suburb,
        postcode,
        state,
        cast(bedrooms as integer)            as bedrooms,
        cast(bathrooms as integer)           as bathrooms,
        cast(land_size_sqm as numeric)       as land_size_sqm,
        property_type,
        cast(year_built as integer)          as year_built,
        cast(listing_price as numeric(14,2)) as listing_price,
        cast(_loaded_at as timestamp)        as load_date,
        'bronze.property_raw'                as record_source
    from source
    where property_id is not null
    order by property_id, _loaded_at desc
),

staged as (
    select
        *,
        md5(cast(property_id as text))   as property_hk,
        md5(concat_ws('||',
            coalesce(address,''),
            coalesce(suburb,''),
            coalesce(postcode,''),
            coalesce(state,''),
            cast(bedrooms as text),
            cast(bathrooms as text),
            cast(land_size_sqm as text),
            coalesce(property_type,''),
            cast(year_built as text),
            cast(listing_price as text)
        ))                               as hashdiff
    from deduped
)

select * from staged
