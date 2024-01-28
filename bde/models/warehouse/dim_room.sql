{{
    config(
        unique_key='listing_id'
    )
}}

select * from {{ ref('room_stg') }}