{{
  config(
    materialized='materialized_view'
  )
}}

-- Gold layer: Sales by category with 5-minute tumbling windows
-- Real-time aggregations for category performance monitoring

SELECT
    category,
    TUMBLE_START(event_time, INTERVAL '5' MINUTE) AS window_start,
    TUMBLE_END(event_time, INTERVAL '5' MINUTE) AS window_end,
    COUNT(*) AS transaction_count,
    SUM(quantity) AS total_quantity,
    SUM(sales_amount) AS total_revenue,
    AVG(sales_amount) AS avg_transaction_value,
    SUM(discount_amount) AS total_discount
FROM {{ ref('fact_sales_enriched') }}
GROUP BY 
    category,
    TUMBLE(event_time, INTERVAL '5' MINUTE)
