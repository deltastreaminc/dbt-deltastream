{{
  config(
    materialized='materialized_view'
  )
}}

-- Gold layer: Discount effectiveness analysis
-- Hourly aggregations to measure discount impact on revenue

SELECT
    category,
    TUMBLE_START(event_time, INTERVAL '1' HOUR) AS window_start,
    TUMBLE_END(event_time, INTERVAL '1' HOUR) AS window_end,
    AVG(discount) AS avg_discount_rate,
    SUM(discount_amount) AS total_discount_amount,
    SUM(sales_amount) AS total_revenue,
    SUM(discount_amount) / NULLIF(SUM(sales_amount), 0) AS discount_to_revenue_ratio,
    COUNT(*) AS transaction_count
FROM {{ ref('fact_sales_enriched') }}
GROUP BY 
    category,
    TUMBLE(event_time, INTERVAL '1' HOUR)
