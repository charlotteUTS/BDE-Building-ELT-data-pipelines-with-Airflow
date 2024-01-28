{{
    config(
        unique_key='lga_code'
    )
}}

select * from {{ ref('G01_stg') }}