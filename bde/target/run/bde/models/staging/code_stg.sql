
  create view "postgres"."staging"."code_stg__dbt_tmp"
    
    
  as (
    

with

source  as (

    select * from "postgres"."at3"."nsw_lga_code"

),

lowercase_lga_name as (
    select
        lga_code,
        lower(lga_name) as lowercase_lga_name 
    from source
)


select * from lowercase_lga_name
  );