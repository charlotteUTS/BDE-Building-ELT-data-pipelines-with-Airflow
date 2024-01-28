{{
    config(
        unique_key='listing_id'
    )
}}

select * from {{ ref('property_stg') }}