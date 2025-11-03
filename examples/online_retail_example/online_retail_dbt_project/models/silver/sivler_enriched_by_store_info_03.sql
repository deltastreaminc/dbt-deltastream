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
    event_time_utc,
    transaction_id,
    product_id,
    customer_id,
    store_id,
    quantity,
    unit_price,
    discount,
    sales_amount,
    product_name,
    msrp_price,
    customer_name,
    customer_address_city,
    customer_address_state,
    store_name,
    store_city,
    store_city

FROM {{ source('kafka_infra', 'silver_enriched_by_customer_info_02') }} p
JOIN {{ source('kafka_infra', 'dim_store_sink') }} c WITH
ON p.customer_id = c.customer_id

