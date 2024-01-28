
  create view "postgres"."staging"."host_stg__dbt_tmp"
    
    
  as (
    

with

source  as (

    select * from "postgres"."raw"."host_snapshot"

),

to_date_format as (
    select
        LISTING_ID,
        HOST_ID,
        SCRAPE_ID,
        TO_DATE(scraped_date, 'YYYY-MM-DD') as scraped_date,
        host_name,
        CASE 
            WHEN host_since = 'NaN' THEN NULL
            WHEN host_since = '' THEN NULL
            ELSE TO_DATE(host_since, 'DD/MM/YYYY')
        END as host_since,
        host_is_superhost,
        lower(host_neighbourhood) as host_neighbourhood,
        dbt_scd_id,
        TO_DATE(dbt_updated_at, 'YYYY-MM-DD') as dbt_updated_at,
        TO_DATE(dbt_valid_from, 'YYYY-MM-DD') as dbt_valid_from,
        TO_DATE(dbt_valid_to, 'YYYY-MM-DD') as dbt_valid_to
    from source
    where host_name <> '' and host_since <> '' and host_since <> 'NaN' and host_name <> ''
)

select * from to_date_format
  );