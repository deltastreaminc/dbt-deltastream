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
    c.event_time,
    c.event_time_utc,
    c.transaction_id,
    c.product_id,
    c.customer_id,
    c.store_id,
    c.quantity,
    c.unit_price,
    c.discount,
    c.sales_amount,
    c.product_name,
    c.msrp_price,
    c.customer_name,
    c.customer_address_city,
    c.customer_address_state,
    s.store_name,
    s.store_city,
    s.store_state
FROM {{ ref('silver_enriched_by_customer_info_02') }} c
JOIN {{ ref( 'dim_store_sink') }} s
ON c.store_id = s.store_id

