{{
  config(
    materialized='materialized_view'
  )
}}
SELECT 
  window_start, 
  window_end,
  zipcode, 
  COUNT(*) AS pageviews
FROM HOP ({{ ref('csas_zipcode_pageviews') }}, SIZE 1 minute, ADVANCE BY 30 second)
WITH ('timestamp'='viewtime')
GROUP BY window_start, window_end, zipcode