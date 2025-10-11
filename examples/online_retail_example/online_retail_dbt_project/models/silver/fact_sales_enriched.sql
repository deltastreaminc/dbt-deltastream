{{
  config(
    materialized='stream',
    parameters={
      'topic': 'fact_sales_enriched',
      'value.format': 'JSON',
      'timestamp': 'event_time'
    }
  )
}}

-- Silver layer: Enriched fact stream with dimension context
-- This stream performs real-time JOINs to enrich sales transactions

SELECT
    f.transaction_id,
    f.event_time,
    
    -- Product enrichment
    p.product_name,
    p.category,
    p.brand,
    
    -- Customer enrichment
    c.customer_name,
    c.customer_segment,
    c.loyalty_tier,
    
    -- Store enrichment
    s.store_name,
    s.region,
    s.city,
    
    -- Transaction details
    f.quantity,
    f.unit_price,
    f.discount,
    f.sales_amount,
    (f.sales_amount - (f.quantity * f.unit_price)) AS discount_amount
    
FROM {{ source('kafka_infra', 'fact_sales_raw') }} f
LEFT JOIN {{ ref('dim_product') }} p ON f.product_id = p.product_id
LEFT JOIN {{ ref('dim_customer') }} c ON f.customer_id = c.customer_id
LEFT JOIN {{ ref('dim_store') }} s ON f.store_id = s.store_id
