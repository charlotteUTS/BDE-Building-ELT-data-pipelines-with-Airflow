
  create view "postgres"."warehouse"."fact_census_G01__dbt_tmp"
    
    
  as (
    

select * from "postgres"."staging"."G01_stg"
  );