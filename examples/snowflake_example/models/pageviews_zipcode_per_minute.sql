{{
  config(
    materialized = 'table',
  )
}}

WITH windowed_pageviews AS (
  SELECT 
    time_slice(viewtime, 1, 'MINUTE') as window_start,
    dateadd('minute', 1, time_slice(viewtime, 1, 'MINUTE')) as window_end,
    zipcode,
    COUNT(*) AS pageviews
  FROM {{ ref('csas_zipcode_pageviews') }}
  GROUP BY 1, 2, 3
)

SELECT 
  window_start,
  window_end,
  zipcode,
  pageviews
FROM windowed_pageviews