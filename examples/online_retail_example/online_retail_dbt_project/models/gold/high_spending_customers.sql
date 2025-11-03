{{
  config(
    materialized='materialized_view'
  )
}}
SELECT 
  window_start, 
  window_end,
  customer_id,
  customer_address_city,
  store_city ,
  sum(sales_amount) AS sales_amount,
  sum(quantity) AS quantity
FROM HOP ({{ ref('silver_enriched_by_store_03') }}, SIZE 30 minute, ADVANCE BY 15 ,minute)
WITH ('timestamp'='event_time_utc')
GROUP BY window_start, window_end, customer_id
HAVING sum(sales_amount) >= '{{ var("high_spending_customers_threshold") }}'