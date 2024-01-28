{% snapshot room_snapshot %}

{{
        config(
          target_schema='raw',
          strategy='timestamp',
          unique_key='LISTING_ID',
          updated_at='scraped_date'
        )
    }}

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
from {{ source('raw', 'listings') }}

{% endsnapshot %}