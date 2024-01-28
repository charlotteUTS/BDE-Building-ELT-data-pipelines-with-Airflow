
  create view "postgres"."warehouse"."fact_census_G02__dbt_tmp"
    
    
  as (
    

select * from "postgres"."staging"."G02_stg"
  );