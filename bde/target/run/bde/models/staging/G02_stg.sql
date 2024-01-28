
  create view "postgres"."staging"."G02_stg__dbt_tmp"
    
    
  as (
    

with

source  as (

    select * from "postgres"."raw"."census_g02_nsw_lga"

),

lga as (
    SELECT
    	cast(replace(lga_code_2016, 'LGA', '') as integer) as lga_code,
        Median_age_persons,
        Median_mortgage_repay_monthly,
        Median_tot_prsnl_inc_weekly,
        Median_rent_weekly,
        Median_tot_fam_inc_weekly,
        Average_num_psns_per_bedroom,
        Median_tot_hhd_inc_weekly,
        Average_household_size
    from source
)


select * from lga
  );