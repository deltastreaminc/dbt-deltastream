{{
  config(
    materialized='materialized_view'
  )
}}

-- Gold layer: Store performance with 15-minute tumbling windows
-- Real-time store performance metrics for operational monitoring

SELECT
    store_id,
    store_name,
    region,
    TUMBLE_START(event_time, INTERVAL '15' MINUTE) AS window_start,
    TUMBLE_END(event_time, INTERVAL '15' MINUTE) AS window_end,
    COUNT(*) AS transaction_count,
    SUM(sales_amount) AS total_revenue,
    AVG(sales_amount) AS avg_transaction_value,
    SUM(quantity) AS total_items_sold
FROM {{ ref('fact_sales_enriched') }}
GROUP BY 
    store_id,
    store_name,
    region,
    TUMBLE(event_time, INTERVAL '15' MINUTE)
