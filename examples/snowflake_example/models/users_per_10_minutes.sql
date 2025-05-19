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
        value AS window_start,
        viewtime
    FROM {{ ref('csas_zipcode_pageviews') }},
    LATERAL FLATTEN(
        input => ARRAY_CONSTRUCT(
            DATEADD(
                minute,
                FLOOR(DATE_PART(minute, viewtime) / 10) * 10,
                DATE_TRUNC('hour', viewtime)
            ),
            DATEADD(
                minute,
                FLOOR(DATE_PART(minute, viewtime) / 10) * 10 + 5,
                DATE_TRUNC('hour', viewtime)
            )
        )
    )
),
windowed_data AS (
    SELECT
        window_start,
        DATEADD(minute, 10, window_start) AS window_end,
        zipcode,
        COUNT(DISTINCT userid) AS distinct_users_in_10_minutes
    FROM hopping_windows
    WHERE viewtime >= window_start
      AND viewtime < DATEADD(minute, 10, window_start)
    GROUP BY window_start, DATEADD(minute, 10, window_start), zipcode
)

SELECT
    window_start,
    window_end,
    zipcode,
    distinct_users_in_10_minutes
FROM windowed_data
ORDER BY window_start, zipcode