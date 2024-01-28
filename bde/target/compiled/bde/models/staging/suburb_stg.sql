

with

source  as (


    select * from "postgres"."raw"."nsw_lga_suburb"

),

lowercase_lga_name as (
    select
        lower(lga_name) as lga_name,
        lower(suburb_name) as suburb_name
    from source
)


select * from lowercase_lga_name