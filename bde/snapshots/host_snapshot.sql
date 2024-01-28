{% snapshot host_snapshot %}

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
  HOST_ID,
  SCRAPE_ID,
  SCRAPED_DATE,
  host_name,
  host_since,
  host_is_superhost,
  host_neighbourhood 
from {{ source('raw', 'listings') }}

{% endsnapshot %}