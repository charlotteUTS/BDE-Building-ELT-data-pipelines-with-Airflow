
  create view "postgres"."warehouse"."dim_lge_code_suburb__dbt_tmp"
    
    
  as (
    

SELECT a.lga_code, b.lowercase_lga_name, b.lowercase_suburb_name 
FROM "postgres"."staging"."code_stg" a 
JOIN "postgres"."staging"."suburb_stg" b 
ON a.lowercase_lga_name = b.lowercase_lga_name
  );