

with

source  as (

    select * from "postgres"."raw"."room_snapshot"


),

to_date_format as (
    SELECT
        listing_id,
        scrape_id,
        TO_DATE(scraped_date, 'YYYY-MM-DD') as scraped_date,
        has_availability,
        availability_30,
        number_of_reviews,
        REVIEW_SCORES_RATING,
        REVIEW_SCORES_ACCURACY,
        REVIEW_SCORES_CLEANLINESS,
        REVIEW_SCORES_CHECKIN,
        REVIEW_SCORES_COMMUNICATION,
        REVIEW_SCORES_VALUE,
        dbt_scd_id,
        TO_DATE(dbt_updated_at, 'YYYY-MM-DD') as dbt_updated_at,
        TO_DATE(dbt_valid_from, 'YYYY-MM-DD') as dbt_valid_from,
        TO_DATE(dbt_valid_to, 'YYYY-MM-DD') as dbt_valid_to
    from source
)

select * from to_date_format