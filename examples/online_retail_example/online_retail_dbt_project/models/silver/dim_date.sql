{{
  config(
    materialized='stream',
    parameters={
      'topic': 'dim_date',
      'value.format': 'JSON'
    }
  )
}}

-- Date dimension stream
-- Generated from calendar dates

SELECT
    CAST(FORMAT_DATE('%Y%m%d', date_value) AS INTEGER) AS date_key,
    date_value AS full_date,
    DAYOFWEEK(date_value) AS day_of_week,
    FORMAT_DATE('%A', date_value) AS day_name,
    WEEK(date_value) AS week_of_year,
    MONTH(date_value) AS month,
    FORMAT_DATE('%B', date_value) AS month_name,
    QUARTER(date_value) AS quarter,
    YEAR(date_value) AS year
FROM UNNEST(GENERATE_DATE_ARRAY('2024-01-01', '2026-12-31', INTERVAL 1 DAY)) AS date_value
