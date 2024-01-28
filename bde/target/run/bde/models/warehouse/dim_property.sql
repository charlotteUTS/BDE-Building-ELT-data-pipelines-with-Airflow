
  create view "postgres"."warehouse"."dim_property__dbt_tmp"
    
    
  as (
    

select * from "postgres"."staging"."property_stg"
  );