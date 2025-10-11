{{
  config(
    materialized='materialized_view'
  )
}}

-- Gold layer: Revenue trends across time periods
-- Daily aggregations with calendar dimension context

SELECT
    d.date_key,
    d.day_name,
    d.week_of_year,
    d.month_name,
    d.quarter,
    d.year,
    SUM(f.sales_amount) AS daily_revenue,
    COUNT(*) AS daily_transactions,
    AVG(f.sales_amount) AS avg_transaction_value
FROM {{ ref('fact_sales_enriched') }} f
JOIN {{ ref('dim_date') }} d 
    ON CAST(FORMAT_DATE('%Y%m%d', DATE(f.event_time)) AS INTEGER) = d.date_key
GROUP BY 
    d.date_key,
    d.day_name,
    d.week_of_year,
    d.month_name,
    d.quarter,
    d.year
