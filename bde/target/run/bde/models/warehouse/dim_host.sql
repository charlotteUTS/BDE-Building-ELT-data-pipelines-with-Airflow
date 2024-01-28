
  create view "postgres"."warehouse"."dim_host__dbt_tmp"
    
    
  as (
    

select * from "postgres"."staging"."host_stg"
  );