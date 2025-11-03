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
    address_city as customer_address_city,
    address_state as customer_address_state

FROM {{ source('kafka_infra', 'silver_enriched_by_product_info_01') }} p
JOIN {{ source('kafka_infra', 'dim_customer_sink') }} c WITH
ON p.customer_id = c.customer_id

