{{
  config(
    materialized='materialized_view'
  )
}}

-- Gold layer: Top customers with 24-hour hopping windows
-- Identifies high-value customers with rolling 24-hour metrics

SELECT
    customer_id,
    customer_name,
    customer_segment,
    HOP_START(event_time, INTERVAL '1' HOUR, INTERVAL '24' HOUR) AS window_start,
    HOP_END(event_time, INTERVAL '1' HOUR, INTERVAL '24' HOUR) AS window_end,
    COUNT(*) AS purchase_count,
    SUM(sales_amount) AS total_spent,
    AVG(sales_amount) AS avg_order_value,
    COUNT(DISTINCT category) AS unique_categories
FROM {{ ref('fact_sales_enriched') }}
GROUP BY 
    customer_id,
    customer_name,
    customer_segment,
    HOP(event_time, INTERVAL '1' HOUR, INTERVAL '24' HOUR)
ORDER BY total_spent DESC
LIMIT 100
