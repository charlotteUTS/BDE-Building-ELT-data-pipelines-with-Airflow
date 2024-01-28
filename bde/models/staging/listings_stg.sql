{{
    config(
        unique_key='listing_id'
    )
}}

with

source  as (

    select * from {{ source('raw', 'listings') }}


),

to_date_format as (
    select
        listing_id ,
        scrape_id ,
        TO_DATE(scraped_date, 'YYYY-MM-DD') as scraped_date,
        host_id,
        host_name,
        CASE 
            WHEN host_since = 'NaN' THEN NULL
            WHEN host_since = '' THEN NULL
            ELSE TO_DATE(host_since, 'DD/MM/YYYY')
        END as host_since,
        host_is_superhost,
        lower(host_neighbourhood) as host_neighbourhood,
        lower(listing_neighbourhood) as listing_neighbourhood,
        property_type,
        room_type,
        accommodates,
        price,
        has_availability,
        availability_30,
        number_of_reviews,
        review_scores_rating,
        review_scores_accuracy,
        review_scores_cleanliness,
        review_scores_checkin,
        review_scores_communication,
        review_scores_value
    from source
)

select * from to_date_format