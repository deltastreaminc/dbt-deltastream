{{
  config(
    materialized='materialized_view'
  )
}}
SELECT
  window_start,
  window_end,
  zipcode,
  COUNT(DISTINCT userid) AS distinct_users_in_10_minutes
FROM HOP (
  {{ ref('csas_zipcode_pageviews') }},
  SIZE 10 MINUTE,
  ADVANCE BY 5 MINUTE
)
WITH ('timestamp' = 'viewtime')
GROUP BY window_start, window_end, zipcode