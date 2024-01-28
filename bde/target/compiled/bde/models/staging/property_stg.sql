

with

source  as (

    select * from "postgres"."raw"."property_snapshot"

),

to_date_format as (
    select
        LISTING_ID,
        SCRAPE_ID,
        TO_DATE(scraped_date, 'YYYY-MM-DD') as scraped_date,
        lower(listing_neighbourhood) as listing_neighbourhood,
        property_type,
        room_type,
        accommodates,
        price,
        dbt_scd_id,
        TO_DATE(dbt_updated_at, 'YYYY-MM-DD') as dbt_updated_at,
        TO_DATE(dbt_valid_from, 'YYYY-MM-DD') as dbt_valid_from,
        TO_DATE(dbt_valid_to, 'YYYY-MM-DD') as dbt_valid_to
    from source
)


select * from to_date_format