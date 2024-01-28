{{
    config(
        unique_key='lga_code'
    )
}}

select * from {{ ref('G02_stg') }}