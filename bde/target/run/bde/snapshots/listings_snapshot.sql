
      
  
    

  create  table "postgres"."at3"."listings_snapshot"
  
  
    as
  
  (
    

    select *,
        md5(coalesce(cast(id as varchar ), '')
         || '|' || coalesce(cast(scraped_date as varchar ), '')
        ) as dbt_scd_id,
        scraped_date as dbt_updated_at,
        scraped_date as dbt_valid_from,
        nullif(scraped_date, scraped_date) as dbt_valid_to
    from (
        



select id,listing_id,scrape_id,scraped_date,number_of_reviews,REVIEW_SCORES_RATING,REVIEW_SCORES_ACCURACY,REVIEW_SCORES_CLEANLINESS,REVIEW_SCORES_CHECKIN,REVIEW_SCORES_COMMUNICATION,REVIEW_SCORES_VALUE from "postgres"."at3"."listings"

    ) sbq



  );
  
  