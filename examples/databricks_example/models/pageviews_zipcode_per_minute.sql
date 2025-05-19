{{
    config(
        materialized='table'
    )
}}

WITH hopping_windows AS (
    SELECT
        zipcode,
        -- Generate window_start for each event, for both the current and previous 30s interval
        explode(
            array(
                date_trunc('minute', viewtime) + INTERVAL 0 seconds,
                date_trunc('minute', viewtime) + INTERVAL 30 seconds
            )
        ) AS window_start,
        viewtime
    FROM {{ ref('csas_zipcode_pageviews') }}
),
windowed_data AS (
    SELECT
        window_start,
        window_start + INTERVAL 1 minute AS window_end,
        zipcode,
        COUNT(*) AS pageviews
    FROM hopping_windows
    WHERE viewtime >= window_start
      AND viewtime < window_start + INTERVAL 1 minute
    GROUP BY window_start, window_start + INTERVAL 1 minute, zipcode
)

SELECT
    window_start,
    window_end,
    zipcode,
    pageviews
FROM windowed_data
ORDER BY window_start, zipcode