

with lga as(
SELECT
		listing_id ,
        scraped_date,
        host_id,
        host_name,
		host_since,
        host_is_superhost,
	    host_neighbourhood,
	    c.lga_name as host_neighbourhood_lga_name,        
	    listing_neighbourhood,
		b.lga_name as listing_neighbourhood_lga_name,
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
FROM  "postgres"."staging"."listings_stg" a
left join "postgres"."staging"."suburb_stg" b 
ON a.listing_neighbourhood = b.suburb_name
left join "postgres"."staging"."suburb_stg" c
ON a.host_neighbourhood = c.suburb_name
),
lga_name AS(
select 
		listing_id ,
        scraped_date,
        host_id,
        host_name,
		host_since,
        host_is_superhost,
	    host_neighbourhood,
	    CASE
	        WHEN host_neighbourhood_lga_name is Null THEN host_neighbourhood_lga_name
	        ELSE host_neighbourhood_lga_name end as host_neighbourhood_lga_name,   
	    listing_neighbourhood,    
	    CASE
	        WHEN listing_neighbourhood_lga_name is Null THEN listing_neighbourhood
	        ELSE listing_neighbourhood_lga_name end as listing_neighbourhood_lga_name, 
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
from lga
)
select
		listing_id ,
        scraped_date,
        host_id,
        host_name,
		host_since,
        host_is_superhost,
	    host_neighbourhood,
		host_neighbourhood_lga_name,
	    s.lga_code as host_neighbourhood_lga_code,
	    listing_neighbourhood,    
		listing_neighbourhood_lga_name,    
		ls.lga_code as listing_neighbourhood_lga_code,
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
from lga_name l
left join staging.lga_stg s
on l.host_neighbourhood_lga_name = s.lga_name 
left join staging.lga_stg ls
on l.listing_neighbourhood_lga_name = ls.lga_name