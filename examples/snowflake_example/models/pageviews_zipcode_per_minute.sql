{{
  config(
    materialized='table',
    parameters={
      'store': 'snowflake_store',
      'snowflake.db.name': 'new_db',
      'snowflake.schema.name': 'new_schema'
    }
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