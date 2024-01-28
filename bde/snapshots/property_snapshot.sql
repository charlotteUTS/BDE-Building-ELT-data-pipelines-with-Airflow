{% snapshot property_snapshot %}
{{
  config(
    target_schema='raw',
    strategy='timestamp',
    unique_key='LISTING_ID',
    updated_at='scraped_date'
  )
}}

select
  distinct LISTING_ID,
  SCRAPE_ID,
  scraped_date,
  listing_neighbourhood,
  property_type,
  room_type,
  accommodates,
  price
from {{ source('raw', 'listings') }}

{% endsnapshot %}
