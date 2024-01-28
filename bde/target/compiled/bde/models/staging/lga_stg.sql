

with

source  as (

    select * from "postgres"."raw"."nsw_lga_code"

),

lowercase_lga_name as (
    select
        lga_code,
        lower(lga_name) as lga_name 
    from source
)


select * from lowercase_lga_name