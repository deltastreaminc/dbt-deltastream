{{
  config(
    materialized='stream',
    parameters={
      'value.format': var("value_format"),
      'store':  var("store_name") 
    }
  )
}}

SELECT 
    s.event_time,
    TO_TIMESTAMP(FROM_UNIXTIME(s.event_time)) AS event_time_utc,
    s.transaction_id,
    s.product_id,
    s.customer_id,
    store_id,
    s.quantity,
    s.unit_price,
    s.discount,
    s.sales_amount,
    p.product_name,
    p.product_price as msrp_price
FROM {{ source('kafka_infra', 'fact_sales_raw') }} s with('tim')
JOIN {{ ref( 'dim_product_sink') }} p 
ON s.product_id = p.product_id

