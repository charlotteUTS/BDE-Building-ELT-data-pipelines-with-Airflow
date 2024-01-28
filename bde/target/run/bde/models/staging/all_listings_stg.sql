
  create view "postgres"."staging"."all_listings_stg__dbt_tmp"
    
    
  as (
    

with

source  as (

    select * from "postgres"."at3"."listings"

),

to_date_format as (
    select
        id,
        listing_id,
        scrape_id,
        TO_DATE(scraped_date, 'YYYY-MM-DD') AS scraped_date,
        host_id,
        host_name,
        host_since,
        host_is_superhost,
        lower(host_neighbourhood) as lower_host_neighbourhood,
        listing_neighbourhood,
        accommodates,
        price,
        property_type,
        room_type,
        has_availability, 
        availability_30, 
        number_of_reviews,
        REVIEW_SCORES_RATING,
        REVIEW_SCORES_ACCURACY,
        REVIEW_SCORES_CLEANLINESS,
        REVIEW_SCORES_CHECKIN,
        REVIEW_SCORES_COMMUNICATION,
        REVIEW_SCORES_VALUE
    from source
) 




select * from to_date_format
  );