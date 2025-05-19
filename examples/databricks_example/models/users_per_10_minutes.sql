{{
    config(
        materialized='table'
    )
}}

WITH hopping_windows AS (
    SELECT
        zipcode,
        userid,
        -- Calculate the base 10-minute window start and the "hopping" offset
        explode(
            array(
                date_trunc('hour', viewtime) + INTERVAL '10' MINUTE * floor(minute(viewtime) / 10),
                date_trunc('hour', viewtime) + INTERVAL '10' MINUTE * floor(minute(viewtime) / 10) + INTERVAL '5' MINUTE
            )
        ) AS window_start,
        viewtime
    FROM {{ ref('csas_zipcode_pageviews') }}
),
windowed_data AS (
    SELECT
        window_start,
        window_start + INTERVAL '10' MINUTE AS window_end,
        zipcode,
        COUNT(DISTINCT userid) AS distinct_users_in_10_minutes
    FROM hopping_windows
    WHERE viewtime >= window_start
      AND viewtime < window_start + INTERVAL '10' MINUTE
    GROUP BY window_start, window_start + INTERVAL '10' MINUTE, zipcode
)

SELECT
    window_start,
    window_end,
    zipcode,
    distinct_users_in_10_minutes
FROM windowed_data
ORDER BY window_start, zipcode