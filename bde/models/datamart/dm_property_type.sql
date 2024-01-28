
SELECT
  property_type,
  room_type,
  accommodates,
  CONCAT( EXTRACT(MONTH FROM scraped_date),'/',EXTRACT(YEAR FROM scraped_date)) as scraped_month_year,
  ROUND(SUM(CASE WHEN has_availability = 't' THEN 1 ELSE 0 END)::NUMERIC / NULLIF(COUNT(has_availability), 0) * 100, 2) AS active_listings_rate,
  MIN(CASE WHEN has_availability = 't' THEN price END) AS min_price,
  MAX(CASE WHEN has_availability = 't' THEN price END) AS max_price,
  ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY CASE WHEN has_availability = 't' THEN price END)::NUMERIC, 2) AS median_price,
  ROUND(AVG(CASE WHEN has_availability = 't' THEN price END), 2) AS avg_price,
  COUNT(DISTINCT host_id) AS distinct_host_count,
  ROUND(SUM(CASE WHEN host_is_superhost = 't' THEN 1 ELSE 0 END)::NUMERIC / NULLIF(COUNT(host_is_superhost), 0) * 100, 2) AS superhost_rate,
  ROUND(AVG(CASE WHEN review_scores_rating = 'NaN' THEN 0 ELSE review_scores_rating END),2) as average_review_scores_rating,
  CAST(ROUND(
      (100.0 * (SUM(CASE WHEN has_availability = 't' THEN 1 ELSE 0 END) -
        LAG(SUM(CASE WHEN has_availability = 't' THEN 1 ELSE 0 END), 1) OVER (PARTITION BY property_type,room_type,accommodates ORDER BY EXTRACT(YEAR FROM scraped_date), EXTRACT(MONTH FROM scraped_date)))
      ) / NULLIF(LAG(SUM(CASE WHEN has_availability = 't' THEN 1 ELSE 0 END), 1) OVER (PARTITION BY  property_type,room_type,accommodates ORDER BY EXTRACT(YEAR FROM scraped_date), EXTRACT(MONTH FROM scraped_date)), 0)
   , 2 )
 	AS DECIMAL(10, 2)) AS active_change,
   CAST(ROUND(
      (100.0 * (SUM(CASE WHEN has_availability = 'f' THEN 1 ELSE 0 END) -
        LAG(SUM(CASE WHEN has_availability = 'f' THEN 1 ELSE 0 END), 1) OVER (PARTITION BY property_type,room_type,accommodates ORDER BY EXTRACT(YEAR FROM scraped_date), EXTRACT(MONTH FROM scraped_date)))
      ) / NULLIF(LAG(SUM(CASE WHEN has_availability = 'f' THEN 1 ELSE 0 END), 1) OVER (PARTITION BY  property_type,room_type,accommodates ORDER BY EXTRACT(YEAR FROM scraped_date), EXTRACT(MONTH FROM scraped_date)), 0)
   , 2 )
 	AS DECIMAL(10, 2)) AS inactive_change,	
  (SUM(CASE WHEN has_availability = 't' THEN (30 - availability_30) ELSE 0 END)) AS total_stay,
  ROUND(AVG(CASE WHEN has_availability = 't' THEN (30 - availability_30) * price ELSE 0 END), 2) AS avg_estimated_revenue
FROM {{ ref('fact_listing') }}
GROUP BY
  property_type,
  room_type,
  accommodates,
  EXTRACT(YEAR FROM scraped_date), EXTRACT(MONTH FROM scraped_date)
ORDER BY
  property_type,
  room_type,
  accommodates,
  EXTRACT(YEAR FROM scraped_date), EXTRACT(MONTH FROM scraped_date)