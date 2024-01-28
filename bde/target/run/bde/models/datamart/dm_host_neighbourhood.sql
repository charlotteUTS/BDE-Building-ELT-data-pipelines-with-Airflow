
  create view "postgres"."datamart"."dm_host_neighbourhood__dbt_tmp"
    
    
  as (
    SELECT 
    host_neighbourhood_lga_name,
	CONCAT( EXTRACT(MONTH FROM scraped_date),'/',EXTRACT(YEAR FROM scraped_date)) as scraped_month_year,
    count(DISTINCT host_id) as number_of_distinct_host,
    ROUND(SUM(CASE WHEN has_availability = 't' THEN (30 - availability_30) * price ELSE 0 END), 2) as estimate_revenue_per_active,
    ROUND(SUM(CASE WHEN has_availability = 't' THEN (30 - availability_30) * price ELSE 0 END) / COUNT(DISTINCT host_id), 2) as estimate_revenue_per_host
FROM 
    "postgres"."warehouse"."fact_listing"
GROUP BY 
    host_neighbourhood_lga_name,
    EXTRACT(YEAR FROM scraped_date), EXTRACT(MONTH FROM scraped_date)
ORDER BY 
    host_neighbourhood_lga_name,
    EXTRACT(YEAR FROM scraped_date), EXTRACT(MONTH FROM scraped_date)
  );