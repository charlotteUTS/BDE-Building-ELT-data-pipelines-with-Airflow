{{
    config(
        unique_key='LGA_CODE'
    )
}}

with

source  as (

    select * from {{ source('raw', 'nsw_lga_code') }}

),

lowercase_lga_name as (
    select
        lga_code,
        lower(lga_name) as lga_name 
    from source
)


select * from lowercase_lga_name