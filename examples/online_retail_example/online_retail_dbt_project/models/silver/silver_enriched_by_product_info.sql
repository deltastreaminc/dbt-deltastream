{{
  config(
    materialized='stream',
    parameters={
      'value.format': 'json',
      'key.format': 'JSON',
      'store': 'pubmsk01'
    }
  )
}}

SELECT 
    event_time
    TO_TIMESTAMP(event_time) AS event_time_utc,
    transaction_id,
    product_id,
    customer_id,
    store_id,
    quantity,
    unit_price,
    discount,
    sales_amount,
    product_name,
    product_price as msrp_price
FROM {{ source('kafka_infra', 'fact_sales_raw') }} s
JOIN {{ source('kafka_infra', 'dim_product_sink') }} p 
ON s.product_id = s.product_id

