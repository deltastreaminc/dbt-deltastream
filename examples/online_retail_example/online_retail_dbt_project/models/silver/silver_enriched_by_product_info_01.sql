{{
  config(
    materialized='stream',
    parameters={
      'value.format': '{{ var("value_format") }}',
      'key.format': '{{ var("key_format") }}',
      'store': '{{ var("store_name") }}'
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

