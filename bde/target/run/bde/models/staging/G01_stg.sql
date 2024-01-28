
  create view "postgres"."staging"."G01_stg__dbt_tmp"
    
    
  as (
    

with

source  as (

    select * from "postgres"."raw"."census_g01_nsw_lga"

),

lga as (
    SELECT
    	cast(replace(lga_code_2016, 'LGA', '') as integer) as lga_code,
        *
    from source
)


select * from lga
  );