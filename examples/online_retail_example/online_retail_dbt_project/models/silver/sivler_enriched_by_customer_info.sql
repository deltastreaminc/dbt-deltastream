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
    msrp_price,
    customer_name,
    address_city as customer_address_city
   address_state as customer_address_state

FROM {{ source('kafka_infra', 'silver_enriched_by_product_info') }} p
JOIN {{ source('kafka_infra', 'dim_customer_sink') }} c WITH
ON p.customer_id = c.customer_id

