
      
  
    

  create  table "postgres"."raw"."room_snapshot"
  
  
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
  distinct listing_id,
  scrape_id,
  scraped_date,
  has_availability,
  availability_30,
  number_of_reviews,
  REVIEW_SCORES_RATING,
  REVIEW_SCORES_ACCURACY,
  REVIEW_SCORES_CLEANLINESS,
  REVIEW_SCORES_CHECKIN,
  REVIEW_SCORES_COMMUNICATION,
  REVIEW_SCORES_VALUE 
from "postgres"."raw"."listings"

    ) sbq



  );
  
  