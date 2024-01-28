{{
    config(
        unique_key='listing_id'
    )
}}

select * from {{ ref('host_stg') }}