select
  listing_neighbourhood,
  CONCAT( EXTRACT(MONTH FROM scraped_date),'/',EXTRACT(YEAR FROM scraped_date)) as scraped_month_year,
  round((sum(case when has_availability = 't' then  1 else  0 end )::numeric / count(has_availability)) *100,2) as active_listings_ratio,
  min(case when has_availability = 't' then price end) as minimum_price,
  max(case when has_availability = 't' then price end) as maximum_price,
  round(percentile_cont(0.5) within group (order by case when has_availability = 't' then price end)::numeric,2) as median_price,
  round(avg(case when has_availability = 't' then price end),2) as average_price,
  count(distinct host_id) as distinct_host,
  round((sum(case when host_is_superhost = 't' then  1 else  0 end)::numeric / nullif(count(host_is_superhost),0))*100,2) as superhost_rate,
  ROUND(AVG(CASE WHEN review_scores_rating = 'NaN' THEN 0 ELSE review_scores_rating END),2) as average_review_scores_rating,
  CAST(
    ROUND(
      (
        100.0 * (SUM(CASE WHEN has_availability = 't' THEN 1 ELSE 0 END) -
        LAG(SUM(CASE WHEN has_availability = 't' THEN 1 ELSE 0 END), 1) OVER (PARTITION BY listing_neighbourhood ORDER BY EXTRACT(YEAR FROM scraped_date), EXTRACT(MONTH FROM scraped_date)))
      ) / NULLIF(LAG(SUM(CASE WHEN has_availability = 't' THEN 1 ELSE 0 END), 1) OVER (PARTITION BY listing_neighbourhood ORDER BY EXTRACT(YEAR FROM scraped_date), EXTRACT(MONTH FROM scraped_date)), 0)
   , 2 )
 	AS DECIMAL(10, 2)) AS active_change,
  CAST(
    ROUND(
      (
        100.0 * (SUM(CASE WHEN has_availability = 'f' THEN 1 ELSE 0 END) -
        LAG(SUM(CASE WHEN has_availability = 'f' THEN 1 ELSE 0 END), 1) OVER (PARTITION BY listing_neighbourhood ORDER BY EXTRACT(YEAR FROM scraped_date), EXTRACT(MONTH FROM scraped_date)))
      ) / NULLIF(LAG(SUM(CASE WHEN has_availability = 'f' THEN 1 ELSE 0 END), 1) OVER (PARTITION BY listing_neighbourhood ORDER BY EXTRACT(YEAR FROM scraped_date), EXTRACT(MONTH FROM scraped_date)), 0)
   , 2 )
 	AS DECIMAL(10, 2)) AS inactive_change,
  sum(case when has_availability = 't' then 30 - availability_30  else 0 end) as number_of_stays,
  round(avg(case when has_availability = 't' then (30 - availability_30)*price  else 0 end),2) as avg_estimate_revenue
from
    "postgres"."warehouse"."fact_listing"
group by 
  listing_neighbourhood,
  EXTRACT(YEAR FROM scraped_date), EXTRACT(MONTH FROM scraped_date)
order by 
  listing_neighbourhood, 
  EXTRACT(YEAR FROM scraped_date), EXTRACT(MONTH FROM scraped_date)