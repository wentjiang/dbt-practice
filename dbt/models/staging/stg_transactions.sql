with source as (
    select * from {{ source('bronze', 'property_raw') }}
),

coalesced as (
    select
        *,
        coalesce(sale_type, 'unknown') as sale_type_clean
    from source
),

staged as (
    select
        transaction_id,
        property_id,
        buyer_id,
        seller_id,
        agent_id,
        cast(sale_price as numeric(14,2))     as sale_price,
        cast(sale_date as date)               as sale_date,
        cast(settlement_date as date)         as settlement_date,
        cast(days_on_market as integer)       as days_on_market,
        sale_type_clean                       as sale_type,
        'bronze.property_raw'                 as record_source,
        cast(_loaded_at as timestamp)         as load_date,

        md5(cast(transaction_id as text))     as transaction_hk,
        md5(cast(property_id as text))        as property_hk,
        md5(cast(buyer_id as text))           as buyer_hk,
        md5(cast(seller_id as text))          as seller_hk,
        md5(cast(agent_id as text))           as agent_hk,

        md5(concat_ws('||',
            cast(sale_price as text),
            cast(sale_date as text),
            cast(settlement_date as text),
            cast(days_on_market as text),
            sale_type_clean
        ))                                    as hashdiff
    from coalesced
    where transaction_id is not null
)

select * from staged
