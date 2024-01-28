
      
  
    

  create  table "postgres"."raw"."property_snapshot"
  
  
    as
  
  (
    

    select *,
        md5(coalesce(cast(LISTING_ID as varchar ), '')
         || '|' || coalesce(cast(scraped_date as varchar ), '')
        ) as dbt_scd_id,
        scraped_date as dbt_updated_at,
        scraped_date as dbt_valid_from,
        nullif(scraped_date, scraped_date) as dbt_valid_to
    from (
        


select
  distinct LISTING_ID,
  SCRAPE_ID,
  scraped_date,
  listing_neighbourhood,
  property_type,
  room_type,
  accommodates,
  price
from "postgres"."raw"."listings"

    ) sbq



  );
  
  