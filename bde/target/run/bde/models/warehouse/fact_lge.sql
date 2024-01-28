
  create view "postgres"."warehouse"."fact_lge__dbt_tmp"
    
    
  as (
    

SELECT a.lga_code, b.lga_name, b.suburb_name 
FROM "postgres"."staging"."suburb_stg" b 
LEFT JOIN "postgres"."staging"."lga_stg" a 
ON a.lga_name = b.lga_name
  );