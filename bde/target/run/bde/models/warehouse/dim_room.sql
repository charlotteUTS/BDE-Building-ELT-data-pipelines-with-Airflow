
  create view "postgres"."warehouse"."dim_room__dbt_tmp"
    
    
  as (
    

select * from "postgres"."staging"."room_stg"
  );